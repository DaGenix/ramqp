use std::collections::HashMap;
use std::str;

use byteorder::{ByteOrder, ReadBytesExt, WriteBytesExt, BigEndian};

use nom;
use nom::{be_u8, be_i8, be_u16, be_i16, be_u32, be_i32, be_u64, be_i64, be_f32, be_f64};

use regex::Regex;

use std::io;
use std::io::Write;

lazy_static!{
    static ref NAME_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9-_.:]*$").unwrap();
}

#[repr(u8)]
pub enum FrameType {
    FRAME_METHOD = 1,
    FRAME_HEADER = 2,
    FRAME_BODY = 3,
    FRAME_HEARTBEAT = 8,
}

pub const FRAME_END: u8 = 206;

// TODO - what are these used for?
pub const FRAME_MIN_SIZE: u8 = 4096;
pub const REPLY_SUCCESS: u16 = 200;

#[repr(u16)]
pub enum ErrorCode {
    // Soft errors
    CONTENT_TOO_LARGE = 311,
    NO_CONSUMERS = 313,
    ACCESS_REFUSED = 403,
    NOT_FOUND = 404,
    RESOURCE_LOCKED = 405,
    PRECONDITION_FAILED = 406,

    // Hard errors
    CONNECTION_FORCED = 320,
    INVALID_PATH = 402,
    FRAME_ERROR = 501,
    SYNTAX_ERROR = 502,
    COMMAND_INVALID = 503,
    CHANNEL_ERROR = 504,
    UNEXPECTED_FRAME = 505,
    RESOURCE_ERROR = 506,
    NOT_ALLOWED = 530,
    NOT_IMPLEMENTED = 540,
    INTERNAL_ERROR = 541,
}

#[derive(Debug)]
pub enum Method {
    ConnectionStart {
        version_major: u8,
        version_minor: u8,
        server_properties: HashMap<String, TableFieldValue>,
        mechanisms: Vec<String>,
        locales: Vec<String>,
    },
    ConnectionStartOk {
        mechanism: String,
        response: Vec<u8>,
        locale: String,
    }
}

#[derive(Debug)]
pub struct ContentHeader {
    pub class_id: u16,
    pub weight: u16,
    pub body_size: u64,
    pub property_flags: u16,
    pub properties: HashMap<String, TableFieldValue>,
}

#[derive(Debug)]
pub enum Frame {
    RequiredProtocol(u8, u8, u8),
    Method(u16, Method),
    ContentHeader(u16, ContentHeader),
    ContentBody(u16, Vec<u8>),
    Heartbeat,
}

#[derive(Debug)]
pub enum TableFieldValue {
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

named!(parse_short_string<&[u8], &str>,
    map_res!(
        length_bytes!(be_u8),
        str::from_utf8
    )
);

named!(parse_long_string<&[u8], &[u8]>,
    length_bytes!(be_u32)
);

fn split_spaces(input: &[u8]) -> Result<Vec<String>, str::Utf8Error> {
    str::from_utf8(input).map(|s| {s.split(' ').map(From::from).collect()})
}

named!(parse_space_seperated_long_string<&[u8], Vec<String> >,
    map_res!(
        length_bytes!(be_u32),
        split_spaces
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
        b"s" => chain!(val: parse_short_string, || {TableFieldValue::ShortString(From::from(val))}) |
        b"S" => chain!(val: parse_long_string, || {TableFieldValue::LongString(From::from(val))}) |
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
            |mut hash_map: HashMap<String, TableFieldValue>, item: (&str, TableFieldValue)| {
                hash_map.insert(From::from(item.0), item.1);
                hash_map
            }
        )
    )
);

pub fn parse_frame(input: &[u8]) -> nom::IResult<&[u8], Frame> {
    enum Header {
        FrameHeader(u8, u16, u32),
        RequiredProtocol(u8, u8, u8),
    }

    let (remaining, header) = try_parse!(input, alt!(
        do_parse!(
            frame_type: be_u8 >>
            channel: be_u16 >>
            size: be_u32 >>
            (Header::FrameHeader(frame_type, channel, size))
        ) |
        do_parse!(
            tag!(b"AMPQ") >>
            major: be_u8 >>
            minor: be_u8 >>
            revision: be_u8 >>
            (Header::RequiredProtocol(major, minor, revision))
        )
    ));

    match header {
        Header::RequiredProtocol(major, minor, revision) => {
            nom::IResult::Done(remaining, Frame::RequiredProtocol(major, minor, revision))
        },
        Header::FrameHeader(frame_type, channel, size) => {
            // We must not negotiate a frame size bigger than usize, so, this
            // cast should be fine.
            // TODO: verify that the frame size is within negotiated limits!
            if remaining.len() == 0 || remaining.len() - 1 < size as usize {
                return nom::IResult::Incomplete(nom::Needed::Unknown);
            }

            const METHOD: u8 = 1;

            const CONNECTION_CLASS: u16 = 10;
            const CONNECTION_START: u16 = 10;

            let (remaining, frame) = try_parse!(
                remaining,
                do_parse!(
                    result: switch!(
                        tuple!(value!(frame_type), be_u16, be_u16),
                        (METHOD, CONNECTION_CLASS, CONNECTION_START) => do_parse!(
                            version_major: be_u8 >>
                            version_minor: be_u8 >>
                            server_properties: parse_table >>
                            mechanisms: parse_space_seperated_long_string >>
                            locales: parse_space_seperated_long_string >>
                            (Frame::Method(
                                channel,
                                Method::ConnectionStart {
                                    version_major: version_major,
                                    version_minor: version_minor,
                                    server_properties: server_properties,
                                    mechanisms: mechanisms,
                                    locales: locales,
                                }
                            ))
                        )
                    ) >>
                    tag!(b"\xCE") >>
                    (result)
                )
            );
            nom::IResult::Done(remaining, frame)

            // nom::IResult::Done(remaining, Frame::Heartbeat)
        }
    }
}


/*
pub fn parse_frame<'a, 'b>(frame_header: &'a FrameHeader, data: &'b [u8])
        -> nom::IResult<&'b [u8], Frame> {
    const METHOD: u8 = 1;

    const CONNECTION_CLASS: u16 = 10;
    const CONNECTION_START: u16 = 10;

    do_parse!(
        frame_header
    );

    /*
//    let parser = closure!(
        chain!(
            result: switch!(
                value!(frame_header.frame_type),
                METHOD => switch!(
                    tuple!(be_u16, be_u16),
                    (CONNECTION_CLASS, CONNECTION_START) => chain!(
                        version_major: be_u8 ~
                        version_minor: be_u8 ~
                        server_properties: parse_table ~
                        mechanisms: parse_space_seperated_long_string ~
                        locales: parse_space_seperated_long_string,
                        || {Frame::Method(Method::ConnectionStart{
                            version_major: version_major,
                            version_minor: version_minor,
                            server_properties: server_properties,
                            mechanisms: mechanisms,
                            locales: locales,
                        })}
                    )
                )
            ) ~
            tag!(b"\xCE"),
            || { result }
        )
//    );
//    parser(data)
    */
}
*/

pub enum FrameWriteError {
    ValueTooBig,
}

impl From<FrameWriteError> for io::Error {
    fn from(err: FrameWriteError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, "Frame Write Error")
    }
}

fn write_frame_helper<F>(frame_type: FrameType, channel: u16, buf: &mut Vec<u8>, frame_writer: F)
        -> Result<(), FrameWriteError>
        where F: FnOnce(&mut Vec<u8>) -> Result<(), FrameWriteError> {
    // frame header
    buf.write_u8(frame_type as u8);
    buf.write_u16::<BigEndian>(channel);
    buf.write_u32::<BigEndian>(0);

    // save enough info so that we can set the
    // size field later.
    let size_pos = buf.len() - 4;
    let orig_buf_size = buf.len();

    frame_writer(buf)?;

    let size = (buf.len() - orig_buf_size) as u32;
    BigEndian::write_u32(&mut buf[size_pos..size_pos+4], size);

    buf.write_u8(0xce);

    Ok(())
}

fn write_method_header(buf: &mut Vec<u8>, class: u16, index: u16) {
    buf.write_u16::<BigEndian>(class);
    buf.write_u16::<BigEndian>(index);
}

fn write_short_string(buf: &mut Vec<u8>, val: &String) -> Result<(), FrameWriteError> {
    if val.len() > 0xff {
        return Err(FrameWriteError::ValueTooBig);
    }
    buf.write_u8(val.len() as u8);
    buf.write(val.as_bytes());
    Ok(())
}

fn write_long_string(buf: &mut Vec<u8>, val: &Vec<u8>) -> Result<(), FrameWriteError> {
    if val.len() > 0xffffffff {
        return Err(FrameWriteError::ValueTooBig);
    }
    buf.write_u32::<BigEndian>(val.len() as u32);
    buf.write(val);
    Ok(())
}

pub fn write_frame(frame: Frame, buf: &mut Vec<u8>) -> Result<(), FrameWriteError> {
    match frame {
        Frame::RequiredProtocol(major, minor, revision) => {
            buf.write(b"AMQP\0");
            buf.write_u8(major);
            buf.write_u8(minor);
            buf.write_u8(revision);
            Ok(())
        }
        Frame::Method(channel, method) => {
            match method {
                Method::ConnectionStartOk{mechanism, response, locale} => {
                    write_frame_helper(FrameType::FRAME_METHOD, channel, buf, |buf| {
                        write_method_header(buf, 10, 11);
                        write_short_string(buf, &mechanism)?;
                        write_long_string(buf, &response)?;
                        write_short_string(buf, &locale)?;
                        Ok(())
                    })
                },
                _ => panic!("Can't handle this frame!")
            }
        },
        _ => panic!("Can't handle this frame!")
    }
}

