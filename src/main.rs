#[macro_use]
extern crate nom;

#[macro_use]
extern crate lazy_static;

extern crate byteorder;
extern crate futures;
extern crate tokio_core;
extern crate regex;

mod protocol;
mod async_loop;

use std::collections::HashMap;
use std::str;
use std::io;
use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::cell::RefCell;

use futures::{Future, Stream, Sink, future, stream};

use tokio_core::reactor::{Core, Handle};
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
    Opening(Box<Future<Item=Channel, Error=std::io::Error> >),
    Opened {
        consumer_state: ConsumerState,
    },
}

struct ConnectionState {
    sender: futures::sync::mpsc::Sender<Frame>,
    channels: HashMap<u16, ChannelState>,
    next_channel: u16,
}

pub struct Connection {
    state: Rc<RefCell<ConnectionState>>,
    tune_params: TuneParams,
}

fn spawn_frame_receiver(
        handle: &Handle,
        connection_state: Rc<RefCell<ConnectionState> >,
        stream: stream::SplitStream<tokio_core::io::Framed<tokio_core::net::TcpStream, RmqCodec> >) {
    fn sender_box<F>(f: F) -> Box<Future<Item=futures::sync::mpsc::Sender<Frame>, Error=()> >
            where F: futures::IntoFuture<Item=futures::sync::mpsc::Sender<Frame>, Error=()>,
                  F::Future: 'static {
        Box::new(f.into_future())
    }

    let sender = connection_state.borrow().sender.clone();
    let s = stream.map_err(|_| panic!()).fold(sender, |sender, frame| {
        println!("Got frame: {:?}", &frame);
        match frame {
            Frame::Method(channel, channel_open @ Method::ChannelOpenOk{..}) => {
                sender_box(Ok(sender))
            },
            Frame::Heartbeat => {
                sender_box(sender.send(Frame::Heartbeat).map_err(|_| panic!()))
            },
            _ => panic!("Unexpected frame"),
        }
    }).map(|_| ());
    handle.spawn(s);
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
                .send(Frame::RequiredProtocol(0, 9, 1)).and_then(|x| x.into_future().map_err(|(x, y)| x))

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
                }).and_then(|x| x.into_future().map_err(|(x, y)| x))

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
                        }).and_then(|x| x.into_future().map_err(|(x, y)| x));
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
                    let (sink, stream) = framed.split();

                    let (sender, receiver) = futures::sync::mpsc::channel(0);

                    let receiver_stream = receiver.fold(sink, |sink, frame| {
                        sink.send(frame).map_err(|x| panic!())
                    }).map(|x| ());

                    handle_for_later.spawn(receiver_stream);

                    let state = Rc::new(RefCell::new(ConnectionState {
                        sender: sender,
                        next_channel: 1,
                        channels: HashMap::new(),
                    }));

                    spawn_frame_receiver(&handle_for_later, state.clone(), stream);

                    Ok(Connection {
                        state: state,
                        tune_params: tune_params,
                    })
                })
        });
        Box::new(f)
    }

    pub fn channel(&self) -> Box<Future<Item=Channel, Error=std::io::Error>> {
        let channel;
        let sender;
        {
            let mut state = self.state.borrow_mut();
            channel = state.next_channel;
            state.next_channel += 1;
            sender = state.sender.clone();
        }
        let frame_max = self.tune_params.frame_max;
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
            connection.channel()
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

