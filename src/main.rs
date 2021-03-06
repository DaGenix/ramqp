#[macro_use]
extern crate nom;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate futures;

extern crate byteorder;
extern crate tokio_core;
extern crate regex;

mod protocol;
mod async_loop;
mod sloppy_timeout_stream;
mod stream_ext;

use stream_ext::next_item;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::str;
use std::io;
use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::error::Error;

use futures::{Future, Stream, Sink, Poll, future, stream};
use futures::future::{IntoFuture, Either};

use tokio_core::reactor::{Core, Handle, Timeout};
use tokio_core::net::TcpStream;
use tokio_core::io::{EasyBuf, Codec, Io};

use protocol::{
    Frame,
    Method,
    ContentHeader,
    BasicProperties,
    parse_frame,
    write_frame,
    default_basic_properties,
};

struct RmqCodec;

impl Codec for RmqCodec {
    type In = Frame;
    type Out = Frame;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Frame>> {
        let (remaining_len, frame) = match parse_frame(buf.as_slice()) {
            nom::IResult::Done(remaining, frame) => (remaining.len(), frame),
            nom::IResult::Incomplete(_) => return Ok(None),
            nom::IResult::Error(err) => return Err(io::Error::new(io::ErrorKind::Other, err.description()))
        };
        let len = buf.len();
        buf.drain_to(len - remaining_len);
        Ok(Some(frame))
    }

    fn encode(&mut self, msg: Frame, buf: &mut Vec<u8>) -> io::Result<()> {
        write_frame(msg, buf)?;
        // println!("SENDING: {:?}", &buf);
        Ok(())
    }
}

struct TuneParams {
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,
}

enum ConsumerState {
    NoConsumer,
    // Openeing(Box<Future<Item=Consumer, Error=std::io::Error>>),
    // Opened(<Box<Sink<SinkItem=Frame, SinkError=std::io::Error>>),
}

enum ChannelState {
    Opening(futures::sync::oneshot::Sender<Channel>),
    Opened {
        consumer_state: ConsumerState,
    },
    Closing,
}

struct ConnectionState {
    sender: futures::sync::mpsc::Sender<Frame>,
    channels: HashMap<u16, Option<ChannelState> >,
    next_channel: u16,
}

pub struct Connection {
    state: Rc<RefCell<ConnectionState>>,
    tune_params: TuneParams,
}

enum RecvError {
    SendError(futures::sync::mpsc::SendError<Frame>),
    IoError(io::Error),
    Other,
}

impl RecvError {
    fn description(&self) -> &str {
        match *self {
            RecvError::SendError(ref err) => err.description(),
            RecvError::IoError(ref err) => err.description(),
            RecvError::Other => "OTHER ERROR",
        }
    }
}

enum FrameReceiverResult<T1, T2> {
    Next(T1),
    Send(T2),
}

impl<I, T1, T2> Future for FrameReceiverResult<T1, T2>
    where T1: Future<Item=I, Error=()>,
          T2: Future<Item=I, Error=futures::sync::mpsc::SendError<Frame>>
{
    type Item = I;
    type Error = RecvError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            FrameReceiverResult::Next(ref mut a) => match a.poll() {
                Ok(x) => Ok(x),
                Err(_) => Err(RecvError::Other),
            },
            FrameReceiverResult::Send(ref mut b) => match b.poll() {
                Ok(x) => Ok(x),
                Err(err) => Err(RecvError::SendError(err)),
            },
        }
    }
}

fn spawn_frame_receiver(
        handle: &Handle,
        connection_state: Rc<RefCell<ConnectionState> >,
        frame_max: u32,
        heartbeat: u16,
        stream: stream::SplitStream<tokio_core::io::Framed<tokio_core::net::TcpStream, RmqCodec> >)
            -> io::Result<()> {
    let sender = connection_state.borrow().sender.clone();
    let stream = sloppy_timeout_stream::SloppyTimeoutStream::new(
        stream,
        Duration::from_secs((2 * heartbeat) as u64),
        handle)?;
    let s = stream
        .map_err(|err| RecvError::IoError(err))
        .fold(sender, move |sender, frame| {
            println!("Got frame: {:?}", &frame);
            match frame {
                Frame::Method(channel, Method::ChannelOpenOk{..}) => {
                    let mut connection_state = connection_state.borrow_mut();
                    match connection_state.channels.entry(channel){
                        Entry::Occupied(mut entry) => {
                            match entry.get_mut().take() {
                                Some(ChannelState::Opening(result)) => {
                                    result.complete(Channel {
                                        sender: Some(sender.clone()),
                                        channel: channel,
                                        frame_max: frame_max,
                                    });
                                    entry.insert(Some(ChannelState::Opened {
                                        consumer_state: ConsumerState::NoConsumer,
                                    }));
                                    FrameReceiverResult::Next(Ok(sender).into_future())
                                },
                                Some(ChannelState::Opened{..}) | Some(ChannelState::Closing) => panic!(),
                                None => panic!(),
                            }
                        },
                        Entry::Vacant(_) => panic!()
                    }
                },
                Frame::Heartbeat => {
                    FrameReceiverResult::Send(sender.send(Frame::Heartbeat))
                },
                _ => panic!("Unexpected frame"),
            }
        })
        .map(|_| ())
        .map_err(|err| println!("ERR: {}", err.description()));
    handle.spawn(s);
    Ok(())
}

fn spawn_frame_sender(
        handle: &Handle,
        mpsc_receiver: futures::sync::mpsc::Receiver<Frame>,
        frame_sink: futures::stream::SplitSink<tokio_core::io::Framed<tokio_core::net::TcpStream, RmqCodec>>) {
    let stream = mpsc_receiver.fold(frame_sink, |frame_sink, frame| {
        frame_sink
            .send(frame)
            .map_err(|err| {
                // Yikes! We couldn't send the frame for
                // some reason. Time to tear down the Connection.
                // TODO - save the error
                ()
            })
    }).then(|_| Ok(()));

    handle.spawn(stream);
}

impl Connection {
    pub fn open<'a, A, S1, S2, S3>(
            handle: &Handle,
            addr: A,
            username: S1,
            password: S2,
            virtual_host: S3) -> Box<Future<Item=Connection, Error=std::io::Error>>
            where A: Into<&'a std::net::SocketAddr>,
                  S1: Into<String>,
                  S2: Into<String>,
                  S3: Into<String> {
        let username = username.into();
        let password = password.into();
        let virtual_host = virtual_host.into();
        let handle_for_later = handle.clone();
        let f = TcpStream::connect(addr.into(), &handle.clone()).and_then(|tcp_stream| {
            let framed = tcp_stream.framed(RmqCodec);
            framed
                // Send the AMPQ version that we support - 0.9.1
                .send(Frame::RequiredProtocol(0, 9, 1)).and_then(next_item)

                // Get back a ConnectionStart frame. Verify the frame and then send out
                // a ConnectionStartOk frame with a username and password.
                .and_then(|(frame, framed)| {
                    match frame {
                        Some(Frame::Method(_, Method::ConnectionStart{..})) => Ok(framed),
                        Some(Frame::RequiredProtocol(major, minor, revision)) =>
                            Err(io::Error::new(io::ErrorKind::Other, "Incompatible protocol version")),
                        _ => Err(io::Error::new(io::ErrorKind::Other, "Unexpected Response"))
                    }
                })
                .and_then(move |framed| {
                    framed.send(Frame::Method(0, Method::ConnectionStartOk {
                        client_properties: HashMap::new(),
                        mechanism: From::from("PLAIN"),
                        response: From::from(format!("\0{}\0{}", username, password).as_bytes()),
                        locale: From::from("en_US"),
                    }))
                }).and_then(next_item)

                // Handle the connection tune method
                .and_then(|(frame, framed)| {
                    match frame {
                        Some(Frame::Method(_, tune_method @ Method::ConnectionTune{..})) => Ok((tune_method, framed)),
                        _ => Err(io::Error::new(io::ErrorKind::Other, "Password Authentication Failed"))
                    }
                })
                .and_then(move |(tune_method, framed)| {
                    if let Method::ConnectionTune{channel_max, frame_max, heartbeat} = tune_method {
                        let tune_params = TuneParams {
                            channel_max: channel_max,
                            frame_max: frame_max,
                            heartbeat: heartbeat,
                        };
                        let send_tune_ok = framed.send(Frame::Method(0, Method::ConnectionTuneOk {
                            channel_max: tune_params.channel_max,
                            frame_max: tune_params.frame_max,
                            heartbeat: tune_params.heartbeat,
                        }));
                        let send_open = send_tune_ok.and_then(|framed| {
                            framed.send(Frame::Method(0, Method::ConnectionOpen {
                                virtual_host: virtual_host,
                                reserved_1: String::new(),
                                reserved_2: true,
                            }))
                        }).and_then(next_item);
                        future::ok(tune_params).join(send_open)
                    } else {
                        panic!("Expected ConnectionTune method");
                    }
                })

                // Handle the connections actually being opened
                .and_then(|(tune_params, (frame, framed))| {
                    match frame {
                        Some(Frame::Method(_, tune_method @ Method::ConnectionOpenOk{..})) => Ok((tune_params, framed)),
                        _ => Err(io::Error::new(io::ErrorKind::Other, "Failed to open connection"))
                    }
                })
                .and_then(move |(tune_params, framed)| {
                    let (frame_sink, frame_stream) = framed.split();

                    let (mpsc_sender, mpsc_receiver) = futures::sync::mpsc::channel(0);

                    let state = Rc::new(RefCell::new(ConnectionState {
                        sender: mpsc_sender,
                        next_channel: 1,
                        channels: HashMap::new(),
                    }));

                    spawn_frame_sender(
                        &handle_for_later,
                        mpsc_receiver,
                        frame_sink);

                    spawn_frame_receiver(
                        &handle_for_later,
                        state.clone(),
                        tune_params.frame_max,
                        tune_params.heartbeat,
                        frame_stream)?;

                    Ok(Connection {
                        state: state,
                        tune_params: tune_params,
                    })
                })
        });
        Box::new(f)
    }

    pub fn channel(&self) -> Box<Future<Item=Channel, Error=std::io::Error> > {
//        let channel;
 //       let sender;
        let mut state = self.state.borrow_mut();
        let channel = state.next_channel;
        state.next_channel += 1;
        let (sender, receiver) = futures::sync::oneshot::channel();
        state.channels.insert(
            channel,
            Some(ChannelState::Opening(sender)));
        let fut = state.sender.clone().send(Frame::Method(channel, Method::ChannelOpen {
            reserved_1: String::new(),
        }))
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to open channel"))
        .and_then(|_| receiver.map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to open channel")));
        Box::new(fut)


        /*
        let fut = sender.send(Frame::Method(channel, Method::ChannelOpen {
            reserved_1: String::new(),
        })).map(move |sender| {
            Channel {
                sender: Some(sender),
                channel: channel,
                frame_max: frame_max,
            }
        }).map_err(|err| {
            io::Error::new(io::ErrorKind::Other, "Failed to open channel")
        });
        Box::new(fut)
        */
    }
}

pub struct Channel {
    sender: Option<futures::sync::mpsc::Sender<Frame>>,
    channel: u16,
    frame_max: u32,
}

impl Channel {
    pub fn basic_publish(
            mut self,
            exchange: String,
            routing_key: String,
            basic_properties: BasicProperties,
            mandatory: bool,
            immediate: bool,
            data: Vec<u8>)
            -> Box<Future<Item=Channel, Error=std::io::Error>> {
        let sender = self.sender.take().unwrap();

        let mut messages = Vec::new();
        messages.push(Ok(Frame::Method(self.channel, Method::BasicPublish {
            reserved_1: 0,
            exchange: exchange,
            routing_key: routing_key,
            mandatory: mandatory,
            immediate: immediate,
        })));
        messages.push(Ok(Frame::ContentHeader(self.channel, ContentHeader {
            class_id: 60,
            weight: 0,
            body_size: data.len() as u64,
            properties: basic_properties,
        })));
        for data_chunk in data.chunks(self.frame_max as usize) {
            messages.push(Ok(Frame::ContentBody(self.channel, From::from(data_chunk))));
        }

        let fut = sender.send_all(stream::iter(messages)).map(move |(sender, _)| {
            self.sender = Some(sender);
            self
        }).map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Failed to open channel")
        });

        Box::new(fut)
    }
}

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "127.0.0.1:5672".parse().unwrap();

    let handle_client = Connection::open(
        &core.handle(),
        &address,
        "guest",
        "guest",
        "/").and_then(|connection| {
            connection.channel().map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to open channel"))
        /*
        }).and_then(move |channel| {
            future::ok(channel).join(Timeout::new(Duration::new(1000, 0), &handle))
        });
        */
        }).and_then(|channel| {
            async_loop::async_loop(channel, |channel| {
                // println!("SENDING");
                let mut msg = String::new();
                for _ in 0..1000000 {
                    msg.push('a');
                }
                let x = channel.basic_publish(
                    From::from(""),
                    From::from("palmer_test"),
                    BasicProperties{delivery_mode: Some(2), ..default_basic_properties()},
                    false,
                    false,
                    From::from(msg.as_bytes())
                    //From::from("Hello World!".as_bytes())
                );
                Some(x)
            })
        });

    //let channel = core.run(handle_client);

    core.handle().spawn(handle_client.map(|_| ()).map_err(|_| ()));
    loop { core.turn(None) }
}

