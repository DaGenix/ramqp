use futures::{Future, Stream, Poll};
use futures::stream::StreamFuture;

pub struct NextItem<S> {
    stream: StreamFuture<S>,
}

impl<S: Stream> Future for NextItem<S> {
    type Item = (Option<S::Item>, S);
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.stream.poll() {
            Ok(r) => Ok(r),
            Err((item, stream)) => Err(item),
        }
    }
}

pub fn next_item<S: Stream>(stream: S) -> NextItem<S> {
    NextItem { stream: stream.into_future() }
}

