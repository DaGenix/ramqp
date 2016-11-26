#[macro_use]
extern crate nom;

#[macro_use]
extern crate lazy_static;

extern crate byteorder;
extern crate futures;
extern crate tokio_core;
extern crate regex;

mod protocol;

use std::iter::repeat;
use std::str;

use futures::{BoxFuture, Future, Stream};

use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::io::{EasyBuf, IoFuture, read_exact, write_all, read, Codec, Io};

use std::error::Error;

use std::io;

use protocol::{
    InitialResponse,
    Frame,
    Method,
    parse_initial_response,
    parse_frame,
    parse_frame_header,
    write_frame,
};



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

struct RmqCodec;

impl Codec for RmqCodec {
    type In = Frame;
    type Out = Frame;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Frame>> {
        match parse_initial_response(buf.as_slice()) {
            nom::IResult::Done(_, initial_response) => match initial_response {
                InitialResponse::FrameHeader(frame_header) => {
                    // We make sure to never negotiate a frame size bigger
                    // than usize which makes this safe.
                    if buf.len() < 8 || buf.len() - 8 < frame_header.size as usize {
                        return Ok(None);
                    }
                    buf.drain_to(7);
                    // We make sure to never negotiate a frame size bigger
                    // than usize which makes this safe.
                    match parse_frame(&frame_header, buf.drain_to(frame_header.size as usize + 1).as_slice()) {
                        nom::IResult::Done(_, frame) => Ok(Some(frame)),
                        _ => Err(io::Error::new(io::ErrorKind::Other, "Invalid Frame"))
                    }
                },
                InitialResponse::RequiredProtocol(..) => panic!("Bad protocol")
            },
            nom::IResult::Incomplete(_) => Ok(None),
            _ => Err(io::Error::new(io::ErrorKind::Other, "Invalid Frame"))
        }
    }

    fn encode(&mut self, msg: Frame, buf: &mut Vec<u8>) -> io::Result<()> {
        write_frame(0, msg, buf)?;
        Ok(())
    }
}

fn main() {
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let address = "127.0.0.1:5672".parse().unwrap();

/*
    let handle_client = TcpStream::connect(&address, &handle).and_then(|tcp_stream| {
        write_all(tcp_stream, b"AMQP\0\0\x09\x01").and_then(|(tcp_stream, _)| {
            let buffer: Vec<u8> = repeat(0).take(16384).collect();
            futures::stream::unfold((tcp_stream, buffer), |tcp_stream| {
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
                })
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
*/

    /*
    fn extract_frame(...) -> BoxFuture<(Frame, (TcpStream, Vec<u8>))> {

    }

    fn process_read_data(buffer) -> BoxFuture<(BoxStream<(Frame, Option<Vec<u8>>, Vec<u8>)> {
    }
    */

    /*
    let handle_client = TcpStream::connect(&address, &handle).and_then(|tcp_stream| {
        write_all(tcp_stream, b"AMQP\0\0\x09\x01").and_then(|(tcp_stream, _)| {
            let buffer: Vec<u8> = repeat(0).take(16384).collect();

            futures::stream::unfold((tcp_stream, buffer), |(tcp_stream, buffer)| {
                read(tcp_stream, buffer).and_then(|(tcp_stream, buffer)| {
                    futures::stream::unfold((0, Some(buffer)), |(pos, Some(buffer))| {

                    })


                    match parse_initial_response(&buffer) {
                        nom::IResult::Done(remaining, initial_response) => {
                            match parse_frame(frame_header, &data) {
                                nom::IResult::Done(_, frame) => Ok((frame, (tcp_stream, buffer))),
                                nom::IResult::Incomplete(_) => panic!("Incomplete"),
                                nom::IResult::Error(err) => {
                                    println!("ERROR: {:?}", err);
                                    panic!("Error!")
                                }
                            }
                        },
                        nom::IResult::Incomplete(_) => {

                        },
                        nom::IResult::Error(err) -> {
                            panic!("Invalid response!");
                        }
                    }


                    if let nom::IResult::Done(_, frame_header) = parse_frame_header(&data) {
                        // TODO - prevent overflow?
                        let data: Vec<u8> = repeat(0).take((frame_header.size + 1) as usize).collect();
                        read_exact(tcp_stream, data).and_then(|(tcp_stream, data)| {
                        })
                    } else {
                        panic!("Failed to connect");
                    }

                })
            })

        })
    });

    core.run(handle_client).unwrap();
    */

    let handle_client = TcpStream::connect(&address, &handle).and_then(|tcp_stream| {
        write_all(tcp_stream, b"AMQP\0\0\x09\x01").and_then(|(tcp_stream, _)| {
            println!("Sent data");
            /*
            tcp_stream.framed(RmqCodec).for_each(|frame| {
                println!("FRAME: {:?}", &frame);
                Ok(())
            }).map_err(|err| println!("ERR: {:?}", &err))
            */
            let (sink, stream) = tcp_stream.framed(RmqCodec).split();
            stream.for_each(|frame| {
                println!("FRAME: {:?}", &frame);
                Ok(())
            })
        })
    });

        //.map(|x| {
        //    x.framed(RmqCodec).send(Frame::Method(Method::ConnectionStart(
        //        version_major: 9,
        //        version_minor: 1,
        //        server_properties: HashMap<String, TableFieldValue
        //});
        //.for_each(|frame| {
        //});

    core.run(handle_client).unwrap();
}

