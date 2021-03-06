use std::collections::HashMap;
use std::str;

use byteorder::{ByteOrder, WriteBytesExt, BigEndian};

use nom;
use nom::{be_u8, be_i8, be_u16, be_i16, be_u32, be_i32, be_u64, be_i64, be_f32, be_f64};

use regex::Regex;

use std::io;
use std::io::Write;

lazy_static!{
    static ref NAME_REGEX: Regex = Regex::new(r"^[a-zA-Z0-9-_.:]*$").unwrap();
}

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum FrameType {
    Method = 1,
    Header = 2,
    Body = 3,
    Heartbeat = 8,
}

impl FrameType {
    fn from_primitive(val: u8) -> Option<FrameType> {
        match val {
            1 => Some(FrameType::Method),
            2 => Some(FrameType::Header),
            3 => Some(FrameType::Body),
            8 => Some(FrameType::Heartbeat),
            _ => None
        }
    }
}

pub const FRAME_END: u8 = 206;

// TODO - what are these used for?
// TODO - I don't think I need these
// pub const FRAME_MIN_SIZE: u8 = 4096;

#[repr(u16)]
#[derive(Debug, Clone)]
pub enum ReplyCode {
    // Ok
    ReplySuccess = 200,

    // Soft errors
    ContentTooLarge = 311,
    NoConsumers = 313,
    AccessRefused = 403,
    NotFound = 404,
    ResourceLocked = 405,
    PreconditionFailed = 406,

    // Hard errors
    ConnectionForced = 320,
    InvalidPath = 402,
    FrameError = 501,
    SyntaxError = 502,
    CommandInvalid = 503,
    ChannelError = 504,
    UnexpectedFrame = 505,
    ResourceError = 506,
    NotAllowed = 530,
    NotImplemented = 540,
    InternalError = 541,
}

impl ReplyCode {
    fn from_code(code: u16) -> Option<ReplyCode> {
        match code {
            200 => Some(ReplyCode::ReplySuccess),
            311 => Some(ReplyCode::NoConsumers),
            403 => Some(ReplyCode::AccessRefused),
            404 => Some(ReplyCode::NotFound),
            405 => Some(ReplyCode::ResourceLocked),
            406 => Some(ReplyCode::PreconditionFailed),
            320 => Some(ReplyCode::ConnectionForced),
            402 => Some(ReplyCode::InvalidPath),
            501 => Some(ReplyCode::FrameError),
            502 => Some(ReplyCode::SyntaxError),
            503 => Some(ReplyCode::CommandInvalid),
            504 => Some(ReplyCode::ChannelError),
            505 => Some(ReplyCode::UnexpectedFrame),
            506 => Some(ReplyCode::ResourceError),
            530 => Some(ReplyCode::NotAllowed),
            540 => Some(ReplyCode::NotImplemented),
            541 => Some(ReplyCode::InternalError),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum MethodCode {
    ConnectionStart,
    ConnectionStartOk,
    ConnectionTune,
    ConnectionTuneOk,
    ConnectionOpen,
    ConnectionOpenOk,
    ConnectionClose,
    ConnectionCloseOk,

    ChannelOpen,
    ChannelOpenOk,
    ChannelClose,
    ChannelCloseOk,

    BasicPublish,
}

impl MethodCode {
    fn to_codes(&self) -> (u16, u16) {
        match self {
            &MethodCode::ConnectionStart => (10, 10),
            &MethodCode::ConnectionStartOk => (10, 11),
            &MethodCode::ConnectionTune => (10, 30),
            &MethodCode::ConnectionTuneOk => (10, 31),
            &MethodCode::ConnectionOpen => (10, 40),
            &MethodCode::ConnectionOpenOk => (10, 41),
            &MethodCode::ConnectionClose => (10, 50),
            &MethodCode::ConnectionCloseOk => (10, 51),

            &MethodCode::ChannelOpen => (20, 10),
            &MethodCode::ChannelOpenOk => (20, 11),
            &MethodCode::ChannelClose => (20, 40),
            &MethodCode::ChannelCloseOk => (20, 41),

            &MethodCode::BasicPublish => (60, 40),
        }
    }

    fn from_codes(class: u16, method: u16) -> Option<MethodCode> {
        match (class, method) {
            (10, 10) => Some(MethodCode::ConnectionStart),
            (10, 11) => Some(MethodCode::ConnectionStartOk),
            (10, 30) => Some(MethodCode::ConnectionTune),
            (10, 31) => Some(MethodCode::ConnectionTuneOk),
            (10, 40) => Some(MethodCode::ConnectionOpen),
            (10, 41) => Some(MethodCode::ConnectionOpenOk),
            (10, 50) => Some(MethodCode::ConnectionClose),
            (10, 51) => Some(MethodCode::ConnectionCloseOk),

            (20, 10) => Some(MethodCode::ChannelOpen),
            (20, 11) => Some(MethodCode::ChannelOpenOk),
            (20, 40) => Some(MethodCode::ChannelClose),
            (20, 41) => Some(MethodCode::ChannelCloseOk),

            (60, 40) => Some(MethodCode::BasicPublish),

            _ => None,
        }
    }
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
        client_properties: HashMap<String, TableFieldValue>,
        mechanism: String,
        response: Vec<u8>,
        locale: String,
    },
    ConnectionTune {
        channel_max: u16,
        frame_max: u32,
        heartbeat: u16,
    },
    ConnectionTuneOk {
        channel_max: u16,
        frame_max: u32,
        heartbeat: u16,
    },
    ConnectionOpen {
        virtual_host: String,
        reserved_1: String,
        reserved_2: bool,
    },
    ConnectionOpenOk {
        reserved_1: String,
    },
    ConnectionClose {
        reply_code: ReplyCode,
        reply_text: String,
        method: Option<MethodCode>,
    },
    ConnectionCloseOk,

    ChannelOpen {
        reserved_1: String,
    },
    ChannelOpenOk {
        reserved_1: Vec<u8>,
    },
    ChannelClose {
        reply_code: ReplyCode,
        reply_text: String,
        method: Option<MethodCode>,
    },
    ChannelCloseOk,

    BasicPublish {
        reserved_1: u16,
        exchange: String,
        routing_key: String,
        mandatory: bool,
        immediate: bool,
    },
}

const FLAG_CONTENT_TYPE: u16 = (1 << 15);
const FLAG_CONTENT_ENCODING: u16 = (1 << 14);
const FLAG_HEADERS: u16 = (1 << 13);
const FLAG_DELIVERY_MODE: u16 = (1 << 12);
const FLAG_PRIORITY: u16 = (1 << 11);
const FLAG_CORRELATION_ID: u16 = (1 << 10);
const FLAG_REPLY_TO: u16 = (1 << 9);
const FLAG_EXPIRATION: u16 = (1 << 8);
const FLAG_MESSAGE_ID: u16 = (1 << 7);
const FLAG_TIMESTAMP: u16 = (1 << 6);
const FLAG_TYPE: u16 = (1 << 5);
const FLAG_USER_ID: u16 = (1 << 4);
const FLAG_APP_ID: u16 = (1 << 3);
const FLAG_CLUSTER_ID: u16 = (1 << 2);

#[derive(Debug)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<HashMap<String, TableFieldValue>>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub message_type: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

pub fn default_basic_properties() -> BasicProperties {
    BasicProperties {
        content_type: None,
        content_encoding: None,
        headers: None,
        delivery_mode: None,
        priority: None,
        correlation_id: None,
        reply_to: None,
        expiration: None,
        message_id: None,
        timestamp: None,
        message_type: None,
        user_id: None,
        app_id: None,
        cluster_id: None,
    }
}

#[derive(Debug)]
pub struct ContentHeader {
    pub class_id: u16,
    pub weight: u16,
    pub body_size: u64,
    // In theory, there can be different types of properties
    // In practice, though, only BasicProperties exists.
    pub properties: BasicProperties,
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

#[allow(unused_variables)]
named!(parse_field_value<&[u8], TableFieldValue>,
    switch!(
        take!(1),
        b"t" => do_parse!(val: be_u8 >> (TableFieldValue::Boolean(val != 0))) |
        b"b" => do_parse!(val: be_i8 >> (TableFieldValue::I8(val))) |
        b"B" => do_parse!(val: be_u8 >> (TableFieldValue::U8(val))) |
        b"U" => do_parse!(val: be_i16 >> (TableFieldValue::I16(val))) |
        b"u" => do_parse!(val: be_u16 >> (TableFieldValue::U16(val))) |
        b"I" => do_parse!(val: be_i32 >> (TableFieldValue::I32(val))) |
        b"i" => do_parse!(val: be_u32 >> (TableFieldValue::U32(val))) |
        b"L" => do_parse!(val: be_i64 >> (TableFieldValue::I64(val))) |
        b"l" => do_parse!(val: be_u64 >> (TableFieldValue::U64(val))) |
        b"f" => do_parse!(val: be_f32 >> (TableFieldValue::Float(val))) |
        b"d" => do_parse!(val: be_f64 >> (TableFieldValue::Double(val))) |
        b"D" => do_parse!(scale: be_u8 >> val: be_u64 >> (TableFieldValue::Decimal(scale, val))) |
        b"s" => do_parse!(val: parse_short_string >> (TableFieldValue::ShortString(From::from(val)))) |
        b"S" => do_parse!(val: parse_long_string >> (TableFieldValue::LongString(From::from(val)))) |
        b"A" => do_parse!(
            val: flat_map!(length_bytes!(be_u32), many0!(parse_field_value)) >>
            (TableFieldValue::Array(val))) |
        b"T" => do_parse!(val: be_i64 >> (TableFieldValue::Timestamp(val))) |
        b"F" => do_parse!(val: parse_table >> (TableFieldValue::Table(val))) |
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
    #[derive(Debug)]
    enum Header {
        FrameHeader(FrameType, u16, u32),
        RequiredProtocol(u8, u8, u8),
    }

    let (remaining, header) = try_parse!(input, alt!(
        do_parse!(
            tag!(b"AMQP\0") >>
            major: be_u8 >>
            minor: be_u8 >>
            revision: be_u8 >>
            (Header::RequiredProtocol(major, minor, revision))
        ) |
        do_parse!(
            frame_type: map_opt!(be_u8, FrameType::from_primitive) >>
            channel: be_u16 >>
            size: be_u32 >>
            (Header::FrameHeader(frame_type, channel, size))
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

            // println!("BUF: {:?}", remaining);

            let (remaining, frame) = match frame_type {
                FrameType::Method => try_parse!(
                    remaining,
                    do_parse!(
                        result: switch!(
                            map_opt!(tuple!(be_u16, be_u16), |t: (u16, u16)| MethodCode::from_codes(t.0, t.1)),
                            MethodCode::ConnectionStart => do_parse!(
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
                            ) |
                            MethodCode::ConnectionTune => do_parse!(
                                channel_max: be_u16 >>
                                frame_max: be_u32 >>
                                heartbeat: be_u16 >>
                                (Frame::Method(
                                    channel,
                                    Method::ConnectionTune {
                                        channel_max: channel_max,
                                        frame_max: frame_max,
                                        heartbeat: heartbeat,
                                    }
                                ))
                            ) |
                            MethodCode::ConnectionOpenOk => do_parse!(
                                reserved_1: parse_short_string >>
                                (Frame::Method(
                                    channel,
                                    Method::ConnectionOpenOk {
                                        reserved_1: From::from(reserved_1),
                                    }
                                ))
                            ) |
                            MethodCode::ConnectionClose => do_parse!(
                                reply_code: map_opt!(be_u16, ReplyCode::from_code) >>
                                reply_text: parse_short_string >>
                                class_id: be_u16 >>
                                method_id: be_u16 >>
                                (Frame::Method(
                                    channel,
                                    Method::ConnectionClose {
                                        reply_code: reply_code,
                                        reply_text: From::from(reply_text),
                                        method: MethodCode::from_codes(class_id, method_id),
                                    }
                                ))
                            ) |
                            MethodCode::ConnectionCloseOk => value!(Frame::Method(channel, Method::ConnectionCloseOk)) |
                            MethodCode::ChannelOpenOk => do_parse!(
                                reserved_1: parse_long_string >>
                                (Frame::Method(
                                    channel,
                                    Method::ChannelOpenOk {
                                        reserved_1: From::from(reserved_1),
                                    }
                                ))
                            ) |
                            MethodCode::ChannelClose => do_parse!(
                                reply_code: map_opt!(be_u16, ReplyCode::from_code) >>
                                reply_text: parse_short_string >>
                                class_id: be_u16 >>
                                method_id: be_u16 >>
                                (Frame::Method(
                                    channel,
                                    Method::ChannelClose {
                                        reply_code: reply_code,
                                        reply_text: From::from(reply_text),
                                        method: MethodCode::from_codes(class_id, method_id),
                                    }
                                ))
                            ) |
                            MethodCode::ChannelCloseOk => value!(Frame::Method(channel, Method::ChannelCloseOk))
                        ) >>
                        tag!(&[FRAME_END]) >>
                        (result)
                    )
                ),
                FrameType::Header => try_parse!(
                    remaining,
                    do_parse!(
                        class_id: be_u16 >>
                        weight: be_u16 >>
                        body_size: be_u64 >>
                        property_flags: be_u16 >>
                        content_type: cond!(property_flags & FLAG_CONTENT_TYPE != 0, parse_short_string) >>
                        content_encoding: cond!(property_flags & FLAG_CONTENT_ENCODING != 0, parse_short_string) >>
                        headers: cond!(property_flags & FLAG_HEADERS != 0, parse_table) >>
                        delivery_mode: cond!(property_flags & FLAG_DELIVERY_MODE != 0, be_u8) >>
                        priority: cond!(property_flags & FLAG_PRIORITY != 0, be_u8) >>
                        correlation_id: cond!(property_flags & FLAG_CORRELATION_ID != 0, parse_short_string) >>
                        reply_to: cond!(property_flags & FLAG_REPLY_TO != 0, parse_short_string) >>
                        expiration: cond!(property_flags & FLAG_EXPIRATION != 0, parse_short_string) >>
                        message_id: cond!(property_flags & FLAG_MESSAGE_ID != 0, parse_short_string) >>
                        timestamp: cond!(property_flags & FLAG_TIMESTAMP != 0, be_u64) >>
                        message_type: cond!(property_flags & FLAG_TYPE != 0, parse_short_string) >>
                        user_id: cond!(property_flags & FLAG_USER_ID != 0, parse_short_string) >>
                        app_id: cond!(property_flags & FLAG_APP_ID != 0, parse_short_string) >>
                        cluster_id: cond!(property_flags & FLAG_CLUSTER_ID != 0, parse_short_string) >>
                        tag!(&[FRAME_END]) >>
                        (Frame::ContentHeader(channel, ContentHeader{
                            class_id: class_id,
                            weight: weight,
                            body_size: body_size,
                            properties: BasicProperties {
                                content_type: content_type.map(From::from),
                                content_encoding: content_encoding.map(From::from),
                                headers: headers,
                                delivery_mode: delivery_mode,
                                priority: priority,
                                correlation_id: correlation_id.map(From::from),
                                reply_to: reply_to.map(From::from),
                                expiration: expiration.map(From::from),
                                message_id: message_id.map(From::from),
                                timestamp: timestamp,
                                message_type: message_type.map(From::from),
                                user_id: user_id.map(From::from),
                                app_id: app_id.map(From::from),
                                cluster_id: cluster_id.map(From::from),
                            },
                        }))
                    )
                ),
                FrameType::Body => try_parse!(
                    remaining,
                    do_parse!(
                        body_fragment: take!(size) >>
                        tag!(&[FRAME_END]) >>
                        (Frame::ContentBody(channel, From::from(body_fragment)))
                    )
                ),
                FrameType::Heartbeat => try_parse!(
                    remaining,
                    do_parse!(
                        // TODO check that the channel number is 0!
                        tag!(&[FRAME_END]) >>
                        (Frame::Heartbeat)
                    )
                )
            };
            nom::IResult::Done(remaining, frame)
        }
    }
}

pub enum FrameWriteError {
    ValueTooBig,
}

impl From<FrameWriteError> for io::Error {
    fn from(_: FrameWriteError) -> io::Error {
        io::Error::new(io::ErrorKind::Other, "Frame Write Error")
    }
}

impl From<io::Error> for FrameWriteError {
    fn from(_: io::Error) -> FrameWriteError {
        panic!("This shouldn't happen. We're just writing to Vecs!");
    }
}

fn write_frame_helper<F>(frame_type: FrameType, channel: u16, buf: &mut Vec<u8>, frame_writer: F)
        -> Result<(), FrameWriteError>
        where F: FnOnce(&mut Vec<u8>) -> Result<(), FrameWriteError> {
    // frame header
    buf.write_u8(frame_type as u8)?;
    buf.write_u16::<BigEndian>(channel)?;
    buf.write_u32::<BigEndian>(0)?;

    // save enough info so that we can set the
    // size field later.
    let size_pos = buf.len() - 4;
    let orig_size = buf.len();

    frame_writer(buf)?;

    let size = (buf.len() - orig_size) as u32;
    BigEndian::write_u32(&mut buf[size_pos..size_pos+4], size);

    buf.write_u8(FRAME_END)?;

    Ok(())
}

fn write_method_header(buf: &mut Vec<u8>, method_code: MethodCode) -> Result<(), FrameWriteError> {
    let (class, index) = method_code.to_codes();
    buf.write_u16::<BigEndian>(class)?;
    buf.write_u16::<BigEndian>(index)?;
    Ok(())
}

fn write_short_string(buf: &mut Vec<u8>, val: &String) -> Result<(), FrameWriteError> {
    if val.len() > 0xff {
        return Err(FrameWriteError::ValueTooBig);
    }
    buf.write_u8(val.len() as u8)?;
    buf.write(val.as_bytes())?;
    Ok(())
}

fn write_long_string(buf: &mut Vec<u8>, val: &Vec<u8>) -> Result<(), FrameWriteError> {
    if val.len() > 0xffffffff {
        return Err(FrameWriteError::ValueTooBig);
    }
    buf.write_u32::<BigEndian>(val.len() as u32)?;
    buf.write(val)?;
    Ok(())
}

fn write_table_field_value(buf: &mut Vec<u8>, val: &TableFieldValue) -> Result<(), FrameWriteError> {
    match val {
        &TableFieldValue::Boolean(val) => {
            buf.write_u8('t' as u8)?;
            buf.write_u8(if val {1} else {0})?;
        },
        &TableFieldValue::I8(val) => {
            buf.write_u8('b' as u8)?;
            buf.write_i8(val)?;
        },
        &TableFieldValue::U8(val) => {
            buf.write_u8('B' as u8)?;
            buf.write_u8(val)?;
        },
        &TableFieldValue::I16(val) => {
            buf.write_u8('U' as u8)?;
            buf.write_i16::<BigEndian>(val)?;
        },
        &TableFieldValue::U16(val) => {
            buf.write_u8('u' as u8)?;
            buf.write_u16::<BigEndian>(val)?;
        },
        &TableFieldValue::I32(val) => {
            buf.write_u8('I' as u8)?;
            buf.write_i32::<BigEndian>(val)?;
        },
        &TableFieldValue::U32(val) => {
            buf.write_u8('i' as u8)?;
            buf.write_u32::<BigEndian>(val)?;
        },
        &TableFieldValue::I64(val) => {
            buf.write_u8('L' as u8)?;
            buf.write_i64::<BigEndian>(val)?;
        },
        &TableFieldValue::U64(val) => {
            buf.write_u8('l' as u8)?;
            buf.write_u64::<BigEndian>(val)?;
        },
        &TableFieldValue::Float(val) => {
            buf.write_u8('f' as u8)?;
            buf.write_f32::<BigEndian>(val)?;
        },
        &TableFieldValue::Double(val) => {
            buf.write_u8('d' as u8)?;
            buf.write_f64::<BigEndian>(val)?;
        },
        &TableFieldValue::Decimal(scale, val) => {
            buf.write_u8('D' as u8)?;
            buf.write_u8(scale)?;
            buf.write_u64::<BigEndian>(val)?;
        },
        &TableFieldValue::ShortString(ref val) => {
            buf.write_u8('s' as u8)?;
            write_short_string(buf, val)?;
        },
        &TableFieldValue::LongString(ref val) => {
            buf.write_u8('S' as u8)?;
            write_long_string(buf, val)?;
        },
        &TableFieldValue::Array(ref val) => {
            buf.write_u8('A' as u8)?;
            let size_pos = buf.len();
            buf.write_u32::<BigEndian>(0)?;
            let orig_size = buf.len();

            for v in val {
                write_table_field_value(buf, v)?;
            }

            let size = (buf.len() - orig_size) as u32;
            BigEndian::write_u32(&mut buf[size_pos..size_pos+4], size);
        },
        &TableFieldValue::Timestamp(val) => {
            buf.write_u8('T' as u8)?;
            buf.write_i64::<BigEndian>(val)?;
        },
        &TableFieldValue::Table(ref val) => {
            buf.write_u8('F' as u8)?;
            write_table(buf, val)?;
        },
        &TableFieldValue::NoField => {
            buf.write_u8('V' as u8)?;
        },
    }
    Ok(())
}

fn write_table(buf: &mut Vec<u8>, val: &HashMap<String, TableFieldValue>) -> Result<(), FrameWriteError> {
    let size_pos = buf.len();
    buf.write_u32::<BigEndian>(0)?;
    let orig_size = buf.len();

    for (key, value) in val {
        write_short_string(buf, key)?;
        write_table_field_value(buf, value)?;
    }

    let size = (buf.len() - orig_size) as u32;
    BigEndian::write_u32(&mut buf[size_pos..size_pos+4], size);

    Ok(())
}

pub fn write_frame(frame: Frame, buf: &mut Vec<u8>) -> Result<(), FrameWriteError> {
    match frame {
        Frame::RequiredProtocol(major, minor, revision) => {
            buf.write_all(b"AMQP\0")?;
            buf.write_u8(major)?;
            buf.write_u8(minor)?;
            buf.write_u8(revision)?;
            Ok(())
        }
        Frame::Method(channel, method) => {
            match method {
                Method::ConnectionStartOk{client_properties, mechanism, response, locale} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ConnectionStartOk)?;
                        write_table(buf, &client_properties)?;
                        write_short_string(buf, &mechanism)?;
                        write_long_string(buf, &response)?;
                        write_short_string(buf, &locale)?;
                        Ok(())
                    })
                },
                Method::ConnectionTuneOk{channel_max, frame_max, heartbeat} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ConnectionTuneOk)?;
                        buf.write_u16::<BigEndian>(channel_max)?;
                        buf.write_u32::<BigEndian>(frame_max)?;
                        buf.write_u16::<BigEndian>(heartbeat)?;
                        Ok(())
                    })
                },
                Method::ConnectionOpen{virtual_host, reserved_1, reserved_2} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ConnectionOpen)?;
                        write_short_string(buf, &virtual_host)?;
                        write_short_string(buf, &reserved_1)?;
                        buf.write_u8(if reserved_2 {1} else {0})?;
                        Ok(())
                    })
                },
                Method::ConnectionClose{reply_code, reply_text, method} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ConnectionClose)?;
                        buf.write_u16::<BigEndian>(reply_code as u16);
                        write_short_string(buf, &reply_text)?;
                        if let Some((class_id, method_id)) = method.as_ref().map(MethodCode::to_codes) {
                            buf.write_u16::<BigEndian>(class_id);
                            buf.write_u16::<BigEndian>(method_id);
                        } else {
                            buf.write_u16::<BigEndian>(0);
                            buf.write_u16::<BigEndian>(0);
                        }
                        Ok(())
                    })
                }
                Method::ConnectionCloseOk => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ConnectionCloseOk)?;
                        Ok(())
                    })
                }
                Method::ChannelOpen{reserved_1} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ChannelOpen)?;
                        write_short_string(buf, &reserved_1)?;
                        Ok(())
                    })
                },
                Method::ChannelClose{reply_code, reply_text, method} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ChannelClose)?;
                        buf.write_u16::<BigEndian>(reply_code as u16);
                        write_short_string(buf, &reply_text)?;
                        if let Some((class_id, method_id)) = method.as_ref().map(MethodCode::to_codes) {
                            buf.write_u16::<BigEndian>(class_id);
                            buf.write_u16::<BigEndian>(method_id);
                        } else {
                            buf.write_u16::<BigEndian>(0);
                            buf.write_u16::<BigEndian>(0);
                        }
                        Ok(())
                    })
                }
                Method::ChannelCloseOk => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::ChannelCloseOk)?;
                        Ok(())
                    })
                }
                Method::BasicPublish{reserved_1, exchange, routing_key, mandatory, immediate} => {
                    write_frame_helper(FrameType::Method, channel, buf, |buf| {
                        write_method_header(buf, MethodCode::BasicPublish)?;
                        buf.write_u16::<BigEndian>(reserved_1)?;
                        write_short_string(buf, &exchange)?;
                        write_short_string(buf, &routing_key)?;
                        buf.write_u8(if mandatory {1} else {0} | if immediate {2} else {0})?;
                        Ok(())
                    })
                },
                _ => panic!("Can't handle this frame!")
            }
        },
        Frame::ContentHeader(channel, header) => {
            write_frame_helper(FrameType::Header, channel, buf, |buf| {
                buf.write_u16::<BigEndian>(header.class_id)?;
                buf.write_u16::<BigEndian>(header.weight)?;
                buf.write_u64::<BigEndian>(header.body_size)?;

                let flag_pos = buf.len();
                let mut flags = 0;
                buf.write_u16::<BigEndian>(0)?;

                if header.properties.content_type.is_some() {
                    flags |= FLAG_CONTENT_TYPE;
                    write_short_string(buf, &header.properties.content_type.as_ref().unwrap())?;
                }
                if header.properties.content_encoding.is_some() {
                    flags |= FLAG_CONTENT_ENCODING;
                    write_short_string(buf, &header.properties.content_encoding.as_ref().unwrap())?;
                }
                if header.properties.headers.is_some() {
                    flags |= FLAG_HEADERS;
                    write_table(buf, &header.properties.headers.as_ref().unwrap())?;
                }
                if header.properties.delivery_mode.is_some() {
                    flags |= FLAG_DELIVERY_MODE;
                    buf.write_u8(*header.properties.delivery_mode.as_ref().unwrap())?;
                }
                if header.properties.priority.is_some() {
                    flags |= FLAG_PRIORITY;
                    buf.write_u8(*header.properties.priority.as_ref().unwrap())?;
                }
                if header.properties.correlation_id.is_some() {
                    flags |= FLAG_CORRELATION_ID;
                    write_short_string(buf, &header.properties.correlation_id.as_ref().unwrap())?;
                }
                if header.properties.reply_to.is_some() {
                    flags |= FLAG_REPLY_TO;
                    write_short_string(buf, &header.properties.reply_to.as_ref().unwrap())?;
                }
                if header.properties.expiration.is_some() {
                    flags |= FLAG_EXPIRATION;
                    write_short_string(buf, &header.properties.expiration.as_ref().unwrap())?;
                }
                if header.properties.message_id.is_some() {
                    flags |= FLAG_MESSAGE_ID;
                    write_short_string(buf, &header.properties.message_id.as_ref().unwrap())?;
                }
                if header.properties.timestamp.is_some() {
                    flags |= FLAG_TIMESTAMP;
                    buf.write_u64::<BigEndian>(*header.properties.timestamp.as_ref().unwrap())?;
                }
                if header.properties.message_type.is_some() {
                    flags |= FLAG_TYPE;
                    write_short_string(buf, &header.properties.message_type.as_ref().unwrap())?;
                }
                if header.properties.user_id.is_some() {
                    flags |= FLAG_USER_ID;
                    write_short_string(buf, &header.properties.user_id.as_ref().unwrap())?;
                }
                if header.properties.app_id.is_some() {
                    flags |= FLAG_APP_ID;
                    write_short_string(buf, &header.properties.app_id.as_ref().unwrap())?;
                }
                if header.properties.cluster_id.is_some() {
                    flags |= FLAG_CLUSTER_ID;
                    write_short_string(buf, &header.properties.cluster_id.as_ref().unwrap())?;
                }

                BigEndian::write_u16(&mut buf[flag_pos..flag_pos+2], flags);

                Ok(())
            })
        },
        Frame::ContentBody(channel, data) => {
            write_frame_helper(FrameType::Body, channel, buf, |buf| {
                buf.write_all(&data)?;
                Ok(())
            })
        },
        Frame::Heartbeat => {
            write_frame_helper(FrameType::Heartbeat, 0, buf, |buf| {
                Ok(())
            })
        }
    }
}

