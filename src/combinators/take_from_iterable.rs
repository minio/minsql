// This file is part of MinSQL
// Copyright (c) 2019 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use futures::{stream, StartSend};
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

pub trait TakeFromIterable {
    fn take_from_iterable(self, amt: u64) -> IterableTaker<Self>
    where
        Self: Sized;
}

impl<S: Stream, F, U> TakeFromIterable for stream::Map<S, F>
where
    F: FnMut(S::Item) -> U,
{
    fn take_from_iterable(self, amt: u64) -> IterableTaker<Self>
    where
        Self: Sized,
    {
        self::new(self, amt)
    }
}

/// A stream combinator which returns a maximum number of elements across multiple batches of
/// elements.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct IterableTaker<S> {
    stream: S,
    remaining: u64,
}

pub fn new<S>(s: S, amt: u64) -> IterableTaker<S>
where
    S: Stream,
{
    IterableTaker {
        stream: s,
        remaining: amt,
    }
}

// Forwarding impl of Sink from the underlying stream
impl<S> Sink for IterableTaker<S>
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

impl<S, T> Stream for IterableTaker<S>
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
