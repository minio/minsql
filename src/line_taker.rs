use futures::StartSend;
use std::ops::Deref;
use tokio::prelude::{Async, Poll, Sink, Stream};

#[macro_export]
macro_rules! try_ready {
    ($e:expr) => {
        match $e {
            Ok(tokio::prelude::Async::Ready(t)) => t,
            Ok(tokio::prelude::Async::NotReady) => return Ok(tokio::prelude::Async::NotReady),
            Err(e) => return Err(From::from(e)),
        }
    };
}

pub fn take_lines<S: Stream>(stream: S, amt: u64) -> LineTaker<S> {
    self::new(stream, amt)
}

/// A stream combinator which returns a maximum number of elements across multiple batches of
/// elements.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct LineTaker<S> {
    stream: S,
    remaining: u64,
}

pub fn new<S>(s: S, amt: u64) -> LineTaker<S>
where
    S: Stream,
{
    LineTaker {
        stream: s,
        remaining: amt,
    }
}

// Forwarding impl of Sink from the underlying stream
impl<S> Sink for LineTaker<S>
where
    S: Sink + Stream,
{
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    fn start_send(&mut self, item: S::SinkItem) -> StartSend<S::SinkItem, S::SinkError> {
        self.stream.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), S::SinkError> {
        self.stream.poll_complete()
    }

    fn close(&mut self) -> Poll<(), S::SinkError> {
        self.stream.close()
    }
}

impl<S, T> Stream for LineTaker<S>
where
    S: Stream,
    S::Item: Deref<Target = [T]>,
    S::Item: std::iter::FromIterator<T>,
    S::Item: std::iter::IntoIterator,
    T: std::clone::Clone,
    <S as futures::stream::Stream>::Item: std::iter::FromIterator<
        <<S as futures::stream::Stream>::Item as std::iter::IntoIterator>::Item,
    >,
{
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<S::Item>, S::Error> {
        if self.remaining <= 0 {
            Ok(Async::Ready(None))
        } else {
            let next = try_ready!(self.stream.poll());
            match next {
                Some(v) => {
                    let len = v.len() as u64;
                    if self.remaining.checked_sub(len) == None {
                        let new_vec: S::Item =
                            v.into_iter().take(self.remaining as usize).collect();
                        self.remaining = 0;
                        return Ok(Async::Ready(Some(new_vec)));
                    } else {
                        self.remaining -= v.len() as u64;
                        return Ok(Async::Ready(Some(v)));
                    }
                }
                None => {
                    self.remaining = 0;
                    return Ok(Async::Ready(next));
                }
            }
        }
    }
}
