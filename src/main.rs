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

use futures::{BoxFuture, Future, Stream, Sink};

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
                framed.send(Frame::Method(0, Method::ConnectionStartOk{
                    client_properties: HashMap::new(),
                    mechanism: From::from("PLAIN"),
                    response: From::from(format!("\0{}\0{}", "guest", "guest").as_bytes()),
                    locale: From::from("en_US"),
                }))
            }).and_then(|x| x.into_future().map_err(|(x, y)| x))

            //
            .and_then(|(frame, framed)| {
                println!("FRAME: {:?}", &frame);
                match frame {
                    Some(_) => Ok(()),
                    None => Err(io::Error::new(io::ErrorKind::Other, "Password Authentication Failed"))
                }
            })
    });

    core.run(handle_client).unwrap();
}

