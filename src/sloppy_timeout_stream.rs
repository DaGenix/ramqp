use std::io;
use std::mem;
use std::time::Duration;

use futures::{Async, Future, Poll};
use futures::stream::Stream;
use tokio_core::reactor::{Handle, Interval};

pub struct SloppyTimeoutStream<S> {
    stream: S,
    had_message: bool,
    interval: Interval,
}

impl<S> SloppyTimeoutStream<S> {
    pub fn new(stream: S, interval_duration: Duration, handle: &Handle) -> io::Result<Self> {
        Ok(SloppyTimeoutStream {
            stream: stream,
            had_message: false,
            interval: Interval::new(interval_duration, handle)?
        })
    }
}

impl<S, I> Stream for SloppyTimeoutStream<S> where S: Stream<Item=I, Error=io::Error> {
    type Item = I;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.stream.poll() {
            r @ Ok(Async::Ready(_)) | r @ Err(_) => {
                self.had_message = true;
                r
            },
            Ok(Async::NotReady) => {
                try_ready!(self.interval.poll());
                if self.had_message {
                    self.had_message = false;
                    Ok(Async::NotReady)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Nothing ready before timeout"))
                }
            }
        }
    }
}

