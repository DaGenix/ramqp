#[macro_use]
extern crate nom;

#[macro_use]
extern crate lazy_static;

extern crate byteorder;
extern crate futures;
extern crate tokio_core;
extern crate regex;

mod protocol;

use std::collections::HashMap;
use std::iter::repeat;
use std::str;

use futures::{BoxFuture, Future, Stream, Sink, future};

use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::io::{EasyBuf, IoFuture, read_exact, write_all, read, Codec, Io};

use std::error::Error;

use std::io;

use protocol::{
    Frame,
    Method,
    parse_frame,
    write_frame,
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
        Ok(())
    }
}

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "127.0.0.1:5672".parse().unwrap();

    struct TuneParams {
        channel_max: u16,
        frame_max: u32,
        heartbeat: u16,
    }

    let handle_client = TcpStream::connect(&address, &handle).and_then(|tcp_stream| {
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
            .and_then(|framed| {
                framed.send(Frame::Method(0, Method::ConnectionStartOk {
                    client_properties: HashMap::new(),
                    mechanism: From::from("PLAIN"),
                    response: From::from(format!("\0{}\0{}", "guest", "guest").as_bytes()),
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
            .and_then(|(tune_method, framed)| {
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
                            virtual_host: From::from("/"),
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
                    Some(Frame::Method(_, tune_method @ Method::ConnectionOpenOk{..})) => Ok(tune_params, framed),
                    _ => Err(io::Error::new(io::ErrorKind::Other, "Failed to open connection"))
                }
            })
            .and_then(|(tune_params, framed)| {
                Ok(())
            })
    });

    core.run(handle_client).unwrap();
}

