#[macro_use]
extern crate nom;

extern crate byteorder;
extern crate futures;
extern crate tokio_core;

mod protocol;

use std::iter::repeat;
use std::str;

use futures::stream::Stream;
use futures::Future;

use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::io::read_exact;
use tokio_core::io::write_all;
use tokio_core::io;
use tokio_core::io::IoFuture;

use protocol::{Frame, Method, parse_frame, parse_frame_header};



/*
enum SendCommand {
    Frame {
        frame_header: FrameHeader,
        frame: Frame,
    },
    Shutdown,
}

struct RmqClient {
    is_open: bool,
    send_channel: futures::stream::Sender<SendCommand, std::io::Error>,

}

struct RmqChannel {

}
*/

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "127.0.0.1:5672".parse().unwrap();

    fn read_frame(tcp_stream: TcpStream) -> IoFuture<(Frame, TcpStream)> {
        read_exact(tcp_stream, [0u8; 7]).and_then(|(tcp_stream, data)| {
            if let nom::IResult::Done(_, frame_header) = parse_frame_header(&data) {
                // TODO - prevent overflow?
                let data: Vec<u8> = repeat(0).take((frame_header.size + 1) as usize).collect();
                read_exact(tcp_stream, data).and_then(|(tcp_stream, data)| {
                    match parse_frame(frame_header, &data) {
                        nom::IResult::Done(_, frame) => Ok((frame, tcp_stream)),
                        nom::IResult::Incomplete(_) => panic!("Incomplete"),
                        nom::IResult::Error(err) => {
                            println!("ERROR: {:?}", err);
                            panic!("Error!")
                        }
                    }
                })
            } else {
                panic!("Failed to connect");
            }
        }).boxed()
    }

    let handle_client = TcpStream::connect(&address, &handle).and_then(|tcp_stream| {
        write_all(tcp_stream, b"AMQP\0\0\x09\x01").and_then(|(tcp_stream, _)| {
            futures::stream::unfold(tcp_stream, |tcp_stream| {
                Some(read_frame(tcp_stream))
            }).for_each(|frame| {
                match frame {
                    Frame::Method(method) => {
                        match method {
                            Method::ConnectionStart{ref mechanisms, ..} => {
                                println!("MECHANISMS: {}", str::from_utf8(mechanisms).unwrap());
                            }
                        }
                    },
                    _ => panic!("Unsupported frame!")
                }
                Ok(())
            })
        })
    });

    core.run(handle_client).unwrap();
}

