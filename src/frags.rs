// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::iter::FusedIterator;
use std::time::Instant;

use constellation_common::retry::Retry;
use constellation_common::retry::RetryResult;

use crate::error::ErrorReportInfo;
use crate::generated::large_obj::LargeObjFragReq;

#[derive(Debug)]
struct Frag {
    nretries: usize,
    when: Instant,
    offset: usize,
    len: usize
}

#[derive(Debug, Eq, PartialEq)]
struct Frags {
    // ISSUE #28: use a good data structure here, like a red-black tree.
    frags: Vec<Frag>
}

pub struct InboundFrags {
    frags: Frags,
    curr: usize,
    data: Vec<u8>
}

pub struct OutboundFrags {
    frags: Frags,
    curr: usize,
    retry: Retry,
    data: Vec<u8>
}

struct FragsIter<'a> {
    frags: &'a mut Frags,
    retry: Retry,
    idx: usize
}

struct FragsBytesIter<'a> {
    frags: &'a mut Frags,
    retry: Retry,
    nbytes: usize,
    idx: usize
}

#[derive(Debug)]
pub enum InboundRecvError {
    OutOfBounds
}

#[derive(Debug)]
pub enum OutboundRecvError {
    OutOfBounds
}

#[derive(Debug)]
pub enum OutboundDataError {
    Empty
}

impl InboundFrags {
    #[inline]
    pub fn new(
        len: usize
    ) -> Self {
        InboundFrags {
            frags: Frags::full(len),
            curr: 0,
            data: vec![0; len]
        }
    }

    #[inline]
    pub fn with_capacity(
        len: usize,
        hint: usize
    ) -> Self {
        InboundFrags {
            frags: Frags::full_with_capacity(len, hint),
            curr: 0,
            data: vec![0; len]
        }
    }

    /// Check if the transfer is complete.
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.frags.is_empty()
    }

    /// Consume this `InboundFrags` if complete and produce the raw
    /// data.
    #[inline]
    pub fn finish(self) -> Result<Vec<u8>, Self> {
        if self.is_finished() {
            Ok(self.data)
        } else {
            Err(self)
        }
    }

    /// Receive fragment data.
    pub fn recv(
        &mut self,
        offset: usize,
        data: &[u8]
    ) -> Result<(), InboundRecvError> {
        let data_end = offset + data.len();

        if data_end <= self.data.len() {
            self.data[offset..data_end].copy_from_slice(data);
            self.frags.remove(offset, data.len());

            Ok(())
        } else {
            Err(InboundRecvError::OutOfBounds)
        }
    }

    /// Attempt to generate requests and acknowledgements for fragments.
    pub fn reqs_acks(
        &mut self,
        buf: &mut [(bool, usize, usize)],
        retry: &Retry
    ) -> RetryResult<usize> {
        let mut curr = 0;
        let mut when: Option<Instant> = None;

        // In case we left off past the last fragment.
        self.curr = if self.frags.is_beyond_last(self.curr) &&
            self.curr < self.data.len() &&
            curr < buf.len()
        {
            // Generate an ack for the gap at the end.
            buf[curr] = (false, self.curr, self.data.len() - self.curr);
            curr += 1;

            0
        } else {
            self.curr
        };

        for frag in self.frags.frags_iter(retry.clone(), self.curr) {
            if curr < buf.len() {
                match frag {
                    (RetryResult::Success(()), offset, len) => {
                        if self.curr < offset {
                            let gap = offset - self.curr;

                            buf[curr] = (false, self.curr, gap);
                            self.curr = offset;
                            curr += 1;

                            if curr >= buf.len() {
                                break;
                            }
                        }

                        buf[curr] = (true, offset, len);
                        self.curr = offset + len;
                        curr += 1;
                    }
                    (RetryResult::Retry(retry), offset, len) => {
                        let retry = when.map_or(retry, |when| when.min(retry));

                        when = Some(retry);

                        if self.curr < offset {
                            let gap = offset - self.curr;

                            buf[curr] = (false, self.curr, gap);
                            self.curr = offset + len;
                            curr += 1;
                        }
                    }
                }
            } else {
                break;
            }
        }

        // Try to add the end part if we're past the last fragment.
        self.curr = if self.frags.is_beyond_last(self.curr) &&
            self.curr < self.data.len() &&
            curr < buf.len()
        {
            // Generate an ack for the gap at the end.
            buf[curr] = (false, self.curr, self.data.len() - self.curr);
            curr += 1;

            0
        } else {
            self.curr
        };

        if curr != 0 {
            RetryResult::Success(curr)
        } else {
            match when {
                Some(when) => RetryResult::Retry(when),
                None => RetryResult::Success(0)
            }
        }
    }
}

impl OutboundFrags {
    #[inline]
    pub fn new(
        retry: Retry,
        data: Vec<u8>
    ) -> Self {
        OutboundFrags {
            frags: Frags::full(data.len()),
            curr: 0,
            retry: retry,
            data: data
        }
    }

    #[inline]
    pub fn with_capacity(
        retry: Retry,
        data: Vec<u8>,
        hint: usize
    ) -> Self {
        OutboundFrags {
            frags: Frags::full_with_capacity(data.len(), hint),
            curr: 0,
            retry: retry,
            data: data
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Get a reference to the `len` bytes of data at `offset`.
    #[inline]
    pub fn data(
        &self,
        offset: usize,
        len: usize
    ) -> Result<&'_ [u8], usize> {
        let data_end = offset + len;

        if data_end <= self.data.len() {
            Ok(&self.data[offset..data_end])
        } else {
            Err(self.data.len())
        }
    }

    pub fn recv_req(
        &mut self,
        req: &LargeObjFragReq
    ) -> Result<(), OutboundRecvError> {
        match req {
            LargeObjFragReq::Ack(req) => {
                self.recv_ack(req.offset as usize, req.len as usize)
            }
            LargeObjFragReq::Need(req) => {
                self.recv_need(req.offset as usize, req.len as usize)
            }
        }
    }

    /// Receive fragment data acknowledgements.
    pub fn recv_ack(
        &mut self,
        offset: usize,
        len: usize
    ) -> Result<(), OutboundRecvError> {
        let data_end = offset + len;

        if data_end <= self.data.len() {
            self.frags.remove(offset, len);

            Ok(())
        } else {
            Err(OutboundRecvError::OutOfBounds)
        }
    }

    /// Receive fragment data requests.
    pub fn recv_need(
        &mut self,
        offset: usize,
        len: usize
    ) -> Result<(), OutboundRecvError> {
        let data_end = offset + len;

        if data_end <= self.data.len() {
            self.frags.insert(offset, len);

            Ok(())
        } else {
            Err(OutboundRecvError::OutOfBounds)
        }
    }

    pub fn offer_frag(
        &mut self,
        max_bytes: usize
    ) -> Result<RetryResult<(usize, usize)>, OutboundDataError> {
        let mut when: Option<Instant> = None;

        // Reset the current offset if needed.
        self.curr = if self.frags.is_beyond_last(self.curr) {
            0
        } else {
            self.curr
        };

        for frag in
            self.frags
                .bytes_iter(self.retry.clone(), self.curr, max_bytes)
        {
            match frag {
                (RetryResult::Success(()), offset, len) => {
                    self.curr = offset + len;

                    return Ok(RetryResult::Success((offset, len)));
                }
                (RetryResult::Retry(retry), offset, len) => {
                    let retry = when.map_or(retry, |when| when.min(retry));

                    self.curr = offset + len;
                    when = Some(retry);
                }
            }
        }

        match when {
            Some(when) => Ok(RetryResult::Retry(when)),
            None => Err(OutboundDataError::Empty)
        }
    }

    /// Attempt to generate data fragments to deliver.
    pub fn data_frags(
        &mut self,
        buf: &mut [(usize, usize)],
        max_bytes: usize
    ) -> Result<RetryResult<usize>, OutboundDataError> {
        let mut curr = 0;
        let mut when: Option<Instant> = None;

        // Reset the current offset if needed.
        self.curr = if self.frags.is_beyond_last(self.curr) {
            0
        } else {
            self.curr
        };

        for frag in
            self.frags
                .bytes_iter(self.retry.clone(), self.curr, max_bytes)
        {
            match frag {
                (RetryResult::Success(()), offset, len) => {
                    if curr < buf.len() {
                        buf[curr] = (offset, len);
                        self.curr = offset + len;
                        curr += 1;
                    } else {
                        break;
                    }
                }
                (RetryResult::Retry(retry), offset, len) => {
                    let retry = when.map_or(retry, |when| when.min(retry));

                    self.curr = offset + len;
                    when = Some(retry);
                }
            }
        }

        if curr != 0 {
            Ok(RetryResult::Success(curr))
        } else {
            match when {
                Some(when) => Ok(RetryResult::Retry(when)),
                None => Err(OutboundDataError::Empty)
            }
        }
    }
}

impl Frags {
    #[inline]
    fn full(len: usize) -> Self {
        Frags {
            frags: vec![Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: len
            }]
        }
    }

    #[inline]
    fn full_with_capacity(
        len: usize,
        hint: usize
    ) -> Self {
        let mut frags = Vec::with_capacity(hint);

        frags.push(Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: len
        });

        Frags { frags: frags }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.frags.is_empty()
    }

    fn is_beyond_last(
        &self,
        offset: usize
    ) -> bool {
        let len = self.frags.len();

        if len > 0 {
            let frag = &self.frags[len - 1];

            frag.offset + frag.len <= offset
        } else {
            true
        }
    }

    #[inline]
    fn frags_iter(
        &mut self,
        retry: Retry,
        offset: usize
    ) -> FragsIter<'_> {
        let idx = match self
            .frags
            .binary_search_by(|frag| frag.offset.cmp(&offset))
        {
            Ok(idx) => idx,
            Err(idx) => idx
        };

        FragsIter {
            frags: self,
            retry: retry,
            idx: idx
        }
    }

    #[inline]
    fn bytes_iter(
        &mut self,
        retry: Retry,
        offset: usize,
        bytes: usize
    ) -> FragsBytesIter<'_> {
        let idx = match self
            .frags
            .binary_search_by(|frag| frag.offset.cmp(&offset))
        {
            Ok(idx) => idx,
            Err(idx) => idx
        };

        FragsBytesIter {
            frags: self,
            retry: retry,
            nbytes: bytes,
            idx: idx
        }
    }

    fn split(
        &mut self,
        retry: &Retry,
        idx: usize,
        len: usize
    ) -> (usize, usize) {
        let nretries = self.frags[idx].nretries;
        let delay = retry.retry_delay(nretries);
        let when = Instant::now() + delay;
        let offset = self.frags[idx].offset;

        if self.frags[idx].len < len {
            self.frags[idx].nretries += 1;
            self.frags[idx].when = when;

            (offset, self.frags[idx].len)
        } else {
            self.frags[idx].offset += len;
            self.frags[idx].len -= len;

            self.frags.insert(
                idx,
                Frag {
                    nretries: nretries,
                    when: when,
                    offset: offset,
                    len: len
                }
            );

            (offset, len)
        }
    }

    fn insert(
        &mut self,
        offset: usize,
        len: usize
    ) {
        let nfrags = self.frags.len();
        let new_end = offset + len;
        let start_idx = match self
            .frags
            .binary_search_by(|frag| frag.offset.cmp(&offset))
        {
            Ok(idx) => idx,
            Err(idx) => {
                if idx != 0 {
                    idx - 1
                } else {
                    idx
                }
            }
        };
        let end_idx = match self.frags.binary_search_by(|frag| {
            let frag_end = frag.offset + frag.len;

            frag_end.cmp(&new_end)
        }) {
            Ok(idx) => idx,
            Err(idx) => idx
        };

        if start_idx > 0 &&
            offset <=
                self.frags[start_idx - 1].offset +
                    self.frags[start_idx - 1].len
        {
            // We can merge into the previous fragment.

            self.frags[start_idx - 1].len += new_end -
                (self.frags[start_idx - 1].offset +
                    self.frags[start_idx - 1].len);

            // Delete the overlapping fragments if needed.
            if start_idx < nfrags {
                let _ = self.frags.drain(start_idx..nfrags);
            }
        } else if end_idx + 1 < nfrags &&
            self.frags[end_idx + 1].offset <= new_end
        {
            // We can merge into the end fragment.
            self.frags[end_idx].offset = offset;

            // Delete the overlapping fragments if needed.
            if start_idx < end_idx {
                let _ = self.frags.drain(start_idx..end_idx);
            }
        } else if start_idx < nfrags {
            // We couldn't merge.

            // Check if we fall after the offset of the first fragment.
            if self.frags[start_idx].offset <= offset {
                // See if we actually fall within the range defined by
                // the end fragment.

                // Check if we fall before the end of the last fragment.
                if nfrags <= end_idx &&
                    offset <=
                        self.frags[nfrags - 1].offset +
                            self.frags[nfrags - 1].len
                {
                    let offset = offset.min(self.frags[start_idx].offset);
                    let len = new_end - offset;

                    // Overwrite the start fragment.
                    self.frags[start_idx] = Frag {
                        offset: offset,
                        len: len,
                        when: Instant::now(),
                        nretries: 0
                    };

                    // Delete the overlapping fragments if needed.
                    if start_idx + 1 < nfrags {
                        let _ = self.frags.drain(start_idx + 1..nfrags);
                    }
                } else if end_idx < nfrags &&
                    offset <=
                        self.frags[end_idx].offset +
                            self.frags[end_idx].len
                {
                    let frag_end =
                        self.frags[end_idx].offset + self.frags[end_idx].len;
                    let offset = offset.min(self.frags[start_idx].offset);
                    let end = new_end.max(frag_end);
                    let len = end - offset;

                    // Overwrite the start fragment.
                    self.frags[start_idx] = Frag {
                        offset: offset,
                        len: len,
                        when: Instant::now(),
                        nretries: 0
                    };

                    // Delete the overlapping fragments if needed.
                    if start_idx < end_idx {
                        let _ = self.frags.drain(start_idx + 1..end_idx + 1);
                    }
                } else {
                    // Insert after the end fragment.
                    self.frags.insert(
                        end_idx,
                        Frag {
                            offset: offset,
                            len: len,
                            when: Instant::now(),
                            nretries: 0
                        }
                    )
                }
            } else if self.frags[start_idx].offset <= new_end {
                // We're out of range, but we can merge.
                self.frags[start_idx].len +=
                    len - (new_end - self.frags[start_idx].offset);
                self.frags[start_idx].offset = offset;

                // Delete the overlapping fragments if needed.
                if start_idx + 1 < end_idx {
                    let _ = self.frags.drain(start_idx + 1..end_idx + 1);
                }
            } else {
                // We're out of range and have to insert.
                self.frags.insert(
                    start_idx,
                    Frag {
                        offset: offset,
                        len: len,
                        when: Instant::now(),
                        nretries: 0
                    }
                )
            }
        } else {
            // The range falls completely outside the existing
            // fragments; add a new one.
            self.frags.push(Frag {
                offset: offset,
                len: len,
                when: Instant::now(),
                nretries: 0
            })
        }
    }

    /// Remove a range of fragments.
    fn remove(
        &mut self,
        offset: usize,
        len: usize
    ) {
        let nfrags = self.frags.len();
        let remove_end = offset + len;
        let start_idx = match self
            .frags
            .binary_search_by(|frag| frag.offset.cmp(&offset))
        {
            Ok(idx) => idx,
            Err(idx) => {
                if idx != 0 {
                    idx - 1
                } else {
                    idx
                }
            }
        };

        // We won't need to do anything if we're already beyond the
        // end of the fragments.
        if start_idx < nfrags {
            let end_idx = match self.frags.binary_search_by(|frag| {
                let frag_end = frag.offset + frag.len;

                frag_end.cmp(&remove_end)
            }) {
                Ok(idx) => idx,
                Err(idx) => idx
            };

            // See if the end index is beyond the end of the fragments.
            if end_idx < nfrags {
                let start_offset = self.frags[start_idx].offset;
                let end_offset = self.frags[end_idx].offset;
                let end_len = self.frags[end_idx].len;
                // Add a prelude if we need one.
                let start_idx = if start_offset < offset {
                    self.frags[start_idx] = Frag {
                        offset: start_offset,
                        len: offset - start_offset,
                        when: Instant::now(),
                        nretries: 0
                    };

                    start_idx + 1
                } else {
                    start_idx
                };

                // See if we need a postlude.
                if end_offset + end_len != remove_end {
                    let remove_end = (offset + len) - end_offset;
                    let postlude = Frag {
                        offset: end_offset + remove_end,
                        len: end_len - remove_end,
                        when: Instant::now(),
                        nretries: 0
                    };

                    // See if we can add reuse an existing fragment.
                    if start_idx <= end_idx {
                        // Check that the ack range doesn't end at the
                        // end fragment's start.
                        if end_offset < offset + len {
                            // Use the first fragment.
                            self.frags[start_idx] = postlude;

                            // Delete the remaining fragments.
                            if start_idx < end_idx {
                                let _ = self
                                    .frags
                                    .drain(start_idx + 1..end_idx + 1);
                            }
                        } else {
                            // This can happen if the ack range ends
                            // exactly at the start of the fragment.
                            let _ = self.frags.drain(start_idx..end_idx);
                        }
                    } else {
                        self.frags.insert(start_idx, postlude);
                    }
                } else if start_idx <= end_idx {
                    // Delete the remaining fragments.
                    let _ = self.frags.drain(start_idx..end_idx + 1);
                }
            } else {
                // Add the prelude if needed.
                let start_offset = self.frags[start_idx].offset;
                // Add a prelude if we have one.
                let start_idx = if start_offset < offset {
                    self.frags[start_idx] = Frag {
                        offset: start_offset,
                        len: offset - start_offset,
                        when: Instant::now(),
                        nretries: 0
                    };

                    start_idx + 1
                } else {
                    start_idx
                };

                // Delete the range if we need to.
                if start_idx < nfrags {
                    let _ = self.frags.drain(start_idx..nfrags);
                }
            }
        }
    }
}

impl Iterator for FragsIter<'_> {
    type Item = (RetryResult<()>, usize, usize);

    #[inline]
    fn next(&mut self) -> Option<(RetryResult<()>, usize, usize)> {
        let idx = self.idx;

        if idx < self.frags.frags.len() {
            let frag = &self.frags.frags[idx];
            let nretries = frag.nretries;
            let offset = frag.offset;
            let len = frag.len;

            self.idx += 1;

            if self.frags.frags[idx].when < Instant::now() {
                self.frags.frags[idx].nretries += 1;

                let delay = self.retry.retry_delay(nretries);
                let when = Instant::now() + delay;

                self.frags.frags[idx].when = when;

                Some((RetryResult::Success(()), offset, len))
            } else {
                Some((
                    RetryResult::Retry(self.frags.frags[idx].when),
                    offset,
                    len
                ))
            }
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint = self.frags.frags.len();

        (0, Some(hint))
    }
}

impl FusedIterator for FragsIter<'_> {}

impl Iterator for FragsBytesIter<'_> {
    type Item = (RetryResult<()>, usize, usize);

    #[inline]
    fn next(&mut self) -> Option<(RetryResult<()>, usize, usize)> {
        let idx = self.idx;

        if idx < self.frags.frags.len() && self.nbytes != 0 {
            if self.frags.frags[idx].when < Instant::now() {
                let (offset, len) =
                    self.frags.split(&self.retry, idx, self.nbytes);

                self.idx += 1;

                if len < self.nbytes {
                    self.nbytes -= len;
                } else {
                    self.nbytes = 0;
                }

                Some((RetryResult::Success(()), offset, len))
            } else {
                Some((
                    RetryResult::Retry(self.frags.frags[idx].when),
                    self.frags.frags[idx].offset,
                    self.frags.frags[idx].len
                ))
            }
        } else {
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let hint = self.frags.frags.len();

        (0, Some(hint))
    }
}

impl FusedIterator for FragsBytesIter<'_> {}

impl PartialEq for Frag {
    fn eq(
        &self,
        other: &Self
    ) -> bool {
        self.offset == other.offset && self.len == other.len
    }
}

impl Eq for Frag {}

impl<Info> ErrorReportInfo<Info> for OutboundDataError {
    #[inline]
    fn report_info(&self) -> Option<Info> {
        match self {
            OutboundDataError::Empty => None
        }
    }
}

impl Display for InboundRecvError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            InboundRecvError::OutOfBounds => {
                write!(f, "data extends beyond bounds")
            }
        }
    }
}

impl Display for OutboundRecvError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            OutboundRecvError::OutOfBounds => {
                write!(f, "range extends beyond data bounds")
            }
        }
    }
}

impl Display for OutboundDataError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            OutboundDataError::Empty => {
                write!(f, "offering from an empty outbound buffer")
            }
        }
    }
}

#[test]
fn test_frags_insert_empty() {
    let mut frags = Frags { frags: vec![] };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_miss_left_nomerge() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 7
        }]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_miss_left_merge() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 16
        }]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_overlap_left_merge() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 7,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 15
        }]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_space_right() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(8, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_exact() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_space_both() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(9, 6);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_space_left() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(9, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_overlap_right_merge() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 15
        }]
    };

    frags.insert(7, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_miss_right_merge() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 16
        }]
    };

    frags.insert(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_single_miss_right_nomerge() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 7
        }]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };

    frags.insert(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_miss_left_nomerge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_miss_left_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 11
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_overlap_left_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 7,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 10
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 3
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(8, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_exact() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_space_both() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(9, 6);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(9, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_overlap_right_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 10
            },
        ]
    };

    frags.insert(7, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_miss_right_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 11
            },
        ]
    };

    frags.insert(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_two_miss_right_nomerge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };

    frags.insert(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_miss_left_nomerge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_miss_left_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 10
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_overlap_left_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 7,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 9
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 2
            },
        ]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(8, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_exact() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.insert(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_space_both() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(9, 6);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.insert(9, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_overlap_right_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 9
            },
        ]
    };

    frags.insert(7, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_miss_right_merge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 2
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 10
            },
        ]
    };

    frags.insert(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_insert_three_miss_right_nomerge() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 1
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };

    frags.insert(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_empty() {
    let mut frags = Frags { frags: vec![] };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_miss_left() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_miss_right() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_exact() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 1,
            len: 8
        }]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_right() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_right() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 1,
            len: 8
        }]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 1
        }]
    };

    frags.remove(1, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_space_right() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 1
        }]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 9
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 1
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_space_left() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 1,
            len: 9
        }]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 1
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_post() {
    let mut frags = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 10
        }]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_exact_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
        ]
    };

    frags.remove(9, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_space_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 9
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 1
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_space_left_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 9
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_post_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 10
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_exact_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 8
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 8
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 10,
            len: 8
        }]
    };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };

    frags.remove(1, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_space_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 9
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_space_left_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 9
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_post_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 10
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_exact_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 8
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(8, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_space_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 9
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_space_left_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 9
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_post_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 10
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_miss_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_miss_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_exact() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 4
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 4
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 1
        }]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 1
        }]
    };

    frags.remove(1, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 5
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 1
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 5
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 1
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_post() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 5
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_exact_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_space_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
        ]
    };

    frags.remove(9, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 1
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_space_left_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 5
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_post_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 5
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_exact_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 8
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 8
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 10,
            len: 8
        }]
    };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(1, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_space_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_space_left_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_post_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_exact_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 8
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_space_left_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(8, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_space_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(9, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_post_space_left_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_two_pre_post_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 5
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_miss_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_miss_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_exact() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 3
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 3
            },
        ]
    };
    let expected = Frags { frags: vec![] };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 1
        }]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 1
        }]
    };

    frags.remove(1, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 1
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 1
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_post() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_exact_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 0,
            len: 8
        }]
    };

    frags.remove(8, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_space_right_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
        ]
    };

    frags.remove(9, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 3
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 1
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_space_left_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_post_before() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 4
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_exact_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 8,
            len: 8
        }]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 8
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 9,
            len: 8
        }]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![Frag {
            when: Instant::now(),
            nretries: 0,
            offset: 10,
            len: 8
        }]
    };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(1, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_space_right_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 3,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            },
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_space_left_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_post_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 6,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            },
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_exact_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 8
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_space_left_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(8, 10);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_space_right_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 11,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(9, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 16,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 8
            },
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_post_space_left_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(8, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_three_pre_post_before_after() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 2
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 14,
                len: 4
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 1
            },
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 18,
                len: 8
            },
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_offer_frag_exact() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 16]);

    let first = frags.offer_frag(16).expect("Expected success");

    assert_eq!(first, RetryResult::Success((0, 16)));
}

#[test]
fn test_offer_frag_short() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 8]);

    let first = frags.offer_frag(16).expect("Expected success");

    assert_eq!(first, RetryResult::Success((0, 8)));
}

#[test]
fn test_offer_frag_multi() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 40]);

    let first = frags.offer_frag(16).expect("Expected success");

    assert_eq!(first, RetryResult::Success((0, 16)));

    let second = frags.offer_frag(16).expect("Expected success");

    assert_eq!(second, RetryResult::Success((16, 16)));

    let third = frags.offer_frag(16).expect("Expected success");

    assert_eq!(third, RetryResult::Success((32, 8)));
}

#[test]
fn test_data_frags_exact() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 16]);
    let mut buf = [(0, 0); 1];

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(1));
    assert_eq!(&buf[0], &(0, 16));
}

#[test]
fn test_data_frags_short() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 8]);
    let mut buf = [(0, 0); 1];

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(1));
    assert_eq!(&buf[0], &(0, 8));
}

#[test]
fn test_data_frags_ack() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 16]);
    let mut buf = [(0, 0); 1];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(1));
    assert_eq!(&buf[0], &(0, 8));
}

#[test]
fn test_data_frags_ack_exact() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 20]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_long() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 24]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_exact_wrap() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 20]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));

    let second = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(second, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_gap_wrap() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 24]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));

    frags.recv_ack(20, 4).expect("Expected success");

    let second = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(second, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));
}

#[test]
fn test_data_frags_ack_req_wrap() {
    let mut frags = OutboundFrags::new(Retry::default(), vec![0; 20]);
    let mut buf = [(0, 0); 2];

    frags.recv_ack(8, 4).expect("Expected success");

    let first = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(0, 8));
    assert_eq!(&buf[1], &(12, 8));

    frags.recv_need(8, 4).expect("Expected success");

    let second = frags.data_frags(&mut buf, 16).expect("Expected success");

    assert_eq!(second, RetryResult::Success(1));
    assert_eq!(&buf[0], &(0, 16));
}

#[test]
fn test_reqs_exact() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 1];

    let first = frags.reqs_acks(&mut buf, &retry);

    assert_eq!(first, RetryResult::Success(1));
    assert_eq!(&buf[0], &(true, 0, 16));
}

#[test]
fn test_reqs_exact_recv_first() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 2];

    frags.recv(0, &[0; 8]).expect("expected success");

    let first = frags.reqs_acks(&mut buf, &retry);

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(false, 0, 8));
    assert_eq!(&buf[1], &(true, 8, 8));
}

#[test]
fn test_reqs_exact_recv_second() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 2];

    frags.recv(8, &[0; 8]).expect("expected success");

    let first = frags.reqs_acks(&mut buf, &retry);

    assert_eq!(first, RetryResult::Success(2));
    assert_eq!(&buf[0], &(true, 0, 8));
    assert_eq!(&buf[1], &(false, 8, 8));
}

#[test]
fn test_reqs_exact_recv_cont() {
    let retry = Retry::default();
    let mut frags = InboundFrags::new(16);
    let mut buf = [(false, 0, 0); 1];

    frags.recv(8, &[0; 8]).expect("expected success");

    let first = frags.reqs_acks(&mut buf, &retry);

    assert_eq!(first, RetryResult::Success(1));
    assert_eq!(&buf[0], &(true, 0, 8));

    let second = frags.reqs_acks(&mut buf, &retry);

    assert_eq!(second, RetryResult::Success(1));
    assert_eq!(&buf[0], &(false, 8, 8));
}
