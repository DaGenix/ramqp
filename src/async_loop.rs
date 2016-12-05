use std::mem;

use futures::{Future, IntoFuture, Async, Poll};

pub fn async_loop<T, F, Fut>(init: T, f: F) -> Loop<T, F, Fut>
    where F: FnMut(T) -> Option<Fut>,
          Fut: IntoFuture<Item = T>,
{
    Loop {
        f: f,
        state: State::Ready(init),
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Loop<T, F, Fut> where Fut: IntoFuture {
    f: F,
    state: State<T, Fut::Future>,
}

impl <T, F, Fut> Future for Loop<T, F, Fut>
    where F: FnMut(T) -> Option<Fut>,
          Fut: IntoFuture<Item = T>,
{
    type Item = ();
    type Error = Fut::Error;

    fn poll(&mut self) -> Poll<(), Fut::Error> {
        loop {
            match mem::replace(&mut self.state, State::Empty) {
                // State::Empty may happen if the future returned an error
                State::Empty => { return Ok(Async::Ready(())); }
                State::Ready(state) => {
                    match (self.f)(state) {
                        Some(fut) => { self.state = State::Processing(fut.into_future()); }
                        None => { return Ok(Async::Ready(())); }
                    }
                }
                State::Processing(mut fut) => {
                    match try!(fut.poll()) {
                        Async::Ready(next_state) => {
                            self.state = State::Ready(next_state);
                        }
                        Async::NotReady => {
                            self.state = State::Processing(fut);
                            return Ok(Async::NotReady);
                        }
                    }
                }
            }
        }
    }
}

enum State<T, F> where F: Future {
    /// Placeholder state when doing work, or when the returned Future generated an error
    Empty,

    /// Ready to generate new future; current internal state is the `T`
    Ready(T),

    /// Working on a future generated previously
    Processing(F),
}

