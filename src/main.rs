#[macro_use]
extern crate nom;

extern crate futures;
extern crate tokio_core;

use std::collections::HashMap;
use std::iter::repeat;
use std::str;

use futures::stream::Stream;
use futures::stream;
use futures::Future;

use tokio_core::reactor::Core;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::io::read_exact;
use tokio_core::io::write_all;
use tokio_core::io;
use tokio_core::io::IoFuture;

use nom::{be_u8, be_i8, be_u16, be_i16, be_u32, be_i32, be_u64, be_i64, be_f32, be_f64};


#[derive(Debug)]
struct FrameHeader {
    frame_type: u8,
    channel: u16,
    size: u32,
}

#[derive(Debug)]
struct MethodHeader {
    class_id: u16,
    method_id: u16,
}

#[derive(Debug)]
struct RequiredProtocol {
    major: u8,
    minor: u8,
    revision: u8,
}

#[derive(Debug)]
enum TableFieldValue {
    Boolean(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    U64(u64),
    Float(f32),
    Double(f64),
    Decimal(u8, u64),
    ShortString(String),
    LongString(Vec<u8>),
    Array(Vec<TableFieldValue>),
    Timestamp(i64),
    Table(HashMap<String, TableFieldValue>),
    NoField,
}

#[derive(Debug)]
enum Method {
    MethodConnectionStart {
        version_major: u8,
        version_minor: u8,
        server_properties: HashMap<String, TableFieldValue>,
        mechanisms: Vec<u8>,
        locales: Vec<u8>,
    },
}

#[derive(Debug)]
enum Frame {
    Method(Method),
    Header,
    Body,
    Heartbeat,
}

named!(parse_required_protocol<&[u8], RequiredProtocol>,
    chain!(
        tag!(b"AMQP") ~
        major: be_u8 ~
        minor: be_u8 ~
        revision: be_u8,
        || { RequiredProtocol{ major: major, minor: minor, revision: revision } }
    )
);

named!(parse_frame_header<&[u8], FrameHeader>,
    chain!(
        frame_type: be_u8 ~
        channel: be_u16 ~
        size: be_u32,
        || { FrameHeader{ frame_type: frame_type, channel: channel, size: size } }
    )
);

named!(parse_short_string<&[u8], String>,
    map_res!(
        map_res!(
            length_bytes!(be_u8),
            str::from_utf8
        ),
        str::FromStr::from_str
    )
);

named!(parse_long_string<&[u8], Vec<u8> >,
    map!(
        length_bytes!(be_u32),
        From::from
    )
);

named!(parse_field_value<&[u8], TableFieldValue>,
    switch!(
        take!(1),
        b"t" => chain!(val: be_u8, || {TableFieldValue::Boolean(val != 0)}) |
        b"b" => chain!(val: be_i8, || {TableFieldValue::I8(val)}) |
        b"B" => chain!(val: be_u8, || {TableFieldValue::U8(val)}) |
        b"U" => chain!(val: be_i16, || {TableFieldValue::I16(val)}) |
        b"u" => chain!(val: be_u16, || {TableFieldValue::U16(val)}) |
        b"I" => chain!(val: be_i32, || {TableFieldValue::I32(val)}) |
        b"i" => chain!(val: be_u32, || {TableFieldValue::U32(val)}) |
        b"L" => chain!(val: be_i64, || {TableFieldValue::I64(val)}) |
        b"l" => chain!(val: be_u64, || {TableFieldValue::U64(val)}) |
        b"f" => chain!(val: be_f32, || {TableFieldValue::Float(val)}) |
        b"d" => chain!(val: be_f64, || {TableFieldValue::Double(val)}) |
        b"D" => chain!(scale: be_u8 ~ val: be_u64, || {TableFieldValue::Decimal(scale, val)}) |
        b"s" => chain!(val: parse_short_string, || {TableFieldValue::ShortString(val)}) |
        b"S" => chain!(val: parse_long_string, || {TableFieldValue::LongString(val)}) |
        b"A" => chain!(
            val: flat_map!(length_bytes!(be_u32), many0!(parse_field_value)),
            || {TableFieldValue::Array(val)}) |
        b"T" => chain!(val: be_i64, || {TableFieldValue::Timestamp(val)}) |
        b"F" => chain!(val: parse_table, || {TableFieldValue::Table(val)}) |
        b"V" => value!(TableFieldValue::NoField)
    )
);

named!(parse_table<&[u8], HashMap<String, TableFieldValue> >,
    flat_map!(
        length_bytes!(be_u32),
        fold_many0!(
            tuple!(
                parse_short_string,
                parse_field_value
            ),
            HashMap::new(),
            |mut hash_map: HashMap<String, TableFieldValue>, item: (String, TableFieldValue)| {
                hash_map.insert(item.0, item.1);
                hash_map
            }
        )
    )
);

named!(parse_method<&[u8], Method>,
    switch!(
        tuple!(be_u16, be_u16),
        (10, 10) => chain!(
            version_major: be_u8 ~
            version_minor: be_u8 ~
            server_properties: parse_table ~
            mechanisms: parse_long_string ~
            locales: parse_long_string ~
            tag!(b"\xCE"),
            || {Method::MethodConnectionStart{
                version_major: version_major,
                version_minor: version_minor,
                server_properties: server_properties,
                mechanisms: mechanisms,
                locales: locales
            }}
        )
    )
);

fn parse_frame(frame_header: FrameHeader, data: &[u8]) -> nom::IResult<&[u8], Frame> {
    const METHOD: u8 = 1;

    const CONNECTION_CLASS: u16 = 10;
    const CONNECTION_START: u16 = 10;

    println!("FRAME TYPE: {}", frame_header.frame_type);

    let parser = closure!(
        chain!(
            result: switch!(
                value!(frame_header.frame_type),
                METHOD => switch!(
                    tuple!(be_u16, be_u16),
                    (CONNECTION_CLASS, CONNECTION_START) => chain!(
                        version_major: be_u8 ~
                        version_minor: be_u8 ~
                        server_properties: parse_table ~
                        mechanisms: parse_long_string ~
                        locales: parse_long_string,
                        || {Frame::Method(Method::MethodConnectionStart{
                            version_major: version_major,
                            version_minor: version_minor,
                            server_properties: server_properties,
                            mechanisms: mechanisms,
                            locales: locales
                        })}
                    )
                )
            ) ~
            tag!(b"\xCE"),
            || { result }
        )
    );
    parser(data)
}

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
                            Method::MethodConnectionStart{ref mechanisms, ..} => {
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

