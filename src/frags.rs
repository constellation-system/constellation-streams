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
use std::time::Instant;

use log::error;

#[derive(Debug)]
enum InboundFrag {
    Data {
        data: Vec<Vec<u8>>,
        offset: usize,
        len: usize
    },
    Needed {
        nretries: usize,
        when: Instant,
        offset: usize,
        len: usize
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct InboundFrags {
    // XXX use a good data structure here, like a splay tree.
    frags: Vec<InboundFrag>
}

#[derive(Debug)]
pub enum InboundInjectError {
    OutOfBounds
}

impl InboundFrags {
    fn add_data_frag_prelude(
        &mut self,
        data: Vec<u8>,
        offset: usize,
        data_len: usize,
        start_idx: usize,
        end_idx: usize
    ) {
        if start_idx <= end_idx {
            // We can replace an existing fragment with the data.
            self.frags[start_idx] = InboundFrag::Data {
                offset: offset,
                len: data_len,
                data: vec![data]
            };

            // Drop any additional fragments.
            if start_idx + 1 <= end_idx {
                let _ = self.frags.drain(start_idx + 1..end_idx + 1);
            }
        } else {
            // There's no space for it.
            self.frags.insert(start_idx, InboundFrag::Data {
                offset: offset,
                len: data_len,
                data: vec![data]
            });
        }
    }

    fn add_data_frag(
        &mut self,
        data: Vec<u8>,
        offset: usize,
        data_len: usize,
        start_idx: usize,
        end_idx: usize
    ) {
        // Replace the first existing fragment with the data.
        self.frags[start_idx] = InboundFrag::Data {
            offset: offset,
            len: data_len,
            data: vec![data]
        };

        // Drop the rest of the fragents.
        if start_idx + 1 < end_idx {
            let _ = self.frags.drain(start_idx + 1..end_idx);
        }
    }

    fn try_right_merge_only(
        &mut self,
        data: Vec<u8>,
        offset: usize,
        data_len: usize,
        start_idx: usize,
        end_idx: usize
    ) {
        if end_idx + 1 < self.frags.len() {
            // See if the previous fragment is a data fragment.
            if let InboundFrag::Data {
                len: frag_len,
                data: frag_data,
                ..
            } = &mut self.frags[end_idx + 1] {
                *frag_len += data_len;
                frag_data.insert(0, data);

                // We merged; drop all the intervening fragments.
                let _ = self.frags.drain(start_idx..end_idx + 1);
            } else {
                self.add_data_frag(data, offset, data_len, start_idx, end_idx)
            }
        } else {
            self.add_data_frag(data, offset, data_len, start_idx, end_idx)
        }
    }

    fn inject(
        &mut self,
        data: Vec<u8>,
        offset: usize
    ) -> Result<(), InboundInjectError> {
        let data_len = data.len();
        let nfrags = self.frags.len();
        let data_end = offset + data_len;
        // Find the start and end indexes.
        let start_idx = match self.frags.binary_search_by(|frag| match frag {
            InboundFrag::Data { offset: frag_offset, .. } =>
                frag_offset.cmp(&offset),
            InboundFrag::Needed { offset: frag_offset, .. } =>
                frag_offset.cmp(&offset)
        }) {
            Ok(idx) => idx,
            Err(idx) => idx - 1
        };
        let end_idx = match self.frags.binary_search_by(|frag| match frag {
            InboundFrag::Data { offset: frag_offset, len: frag_len, .. } => {
                let frag_end = frag_offset + frag_len;

                frag_end.cmp(&data_end)
            },
            InboundFrag::Needed { offset: frag_offset, len: frag_len, .. } => {
                let frag_end = frag_offset + frag_len;

                frag_end.cmp(&data_end)
            }
        }) {
            Ok(idx) => Ok(idx),
            Err(idx) => if idx < nfrags {
                Ok(idx)
            } else {
                Err(InboundInjectError::OutOfBounds)
            }
        }?;
        // Figure out the prelude and postludes.
        let prelude = match &self.frags[start_idx] {
            // The data overlaps the beginning fragment, but there's a
            // leftover prelude.
            InboundFrag::Needed { offset: frag_offset, .. }
            if *frag_offset != offset => Some(InboundFrag::Needed {
                offset: *frag_offset,
                len: offset - frag_offset,
                when: Instant::now(),
                nretries: 0,
            }),
            _ => None
        };
        let postlude = match &self.frags[end_idx] {
            // The data overlaps the ending fragment, but there's a
            // leftover prelude.
            InboundFrag::Needed { offset: frag_offset, len: frag_len, .. }
            if *frag_offset + *frag_len != data_end => {
                let data_end = (offset + data_len) - frag_offset;

                Some(InboundFrag::Needed {
                    offset: frag_offset + data_end,
                    len: frag_len - data_end,
                    when: Instant::now(),
                    nretries: 0,
                })
            }
            _ => None
        };
        // Adjust the start and end indexes if needed.
        let start_idx = if start_idx > 0 {
            if let InboundFrag::Data { .. } = &self.frags[start_idx - 1] &&
                prelude.is_none() {
                start_idx - 1
            } else {
                start_idx
            }
        } else {
            start_idx
        };
        let end_idx = if end_idx + 1 < self.frags.len() {
            if let InboundFrag::Data { .. } = &self.frags[end_idx + 1] &&
                postlude.is_none() {
                end_idx + 1
            } else {
                end_idx
            }
        } else {
            end_idx
        };

        match (prelude, postlude) {
            (Some(prelude), Some(postlude)) => {
                // See if we need to merge in the prelude.
                let start_idx = if start_idx > 0 {
                    if let InboundFrag::Needed {
                        offset: frag_offset,
                        len: frag_len,
                        ..
                    } = &mut self.frags[start_idx - 1] {
                        *frag_len = offset - *frag_offset;

                        start_idx
                    } else {
                        self.frags[start_idx] = prelude;

                        start_idx + 1
                    }
                } else {
                    self.frags[start_idx] = prelude;

                    start_idx + 1
                };

                let data = InboundFrag::Data {
                    offset: offset,
                    len: data_len,
                    data: vec![data]
                };

                let merged = if end_idx + 1 < self.frags.len() {
                    if let InboundFrag::Needed {
                        offset: frag_offset,
                        len: frag_len,
                        ..
                    } = &mut self.frags[end_idx + 1] {
                        *frag_len = (*frag_offset + *frag_len) - (offset + data_len);
                        *frag_offset = offset + data_len;

                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                // See if we can fit the data and postlude.
                if start_idx + 1 <= end_idx {
                    // We can fit both.
                    self.frags[start_idx] = data;

                    if !merged {
                        self.frags[start_idx + 1] = postlude;

                        // Drop any additional fragments.
                        if start_idx + 2 <= end_idx {
                            let _ = self.frags
                                .drain(start_idx + 2..end_idx + 1);
                        }
                    } else {
                        // Drop any additional fragments.
                        if start_idx + 1 <= end_idx {
                            let _ = self.frags
                                .drain(start_idx + 2..end_idx + 1);
                        }
                    }
                } else if start_idx <= end_idx {
                    // We can fit the data, but not the postlude.
                    self.frags[start_idx] = data;

                    if !merged {
                        self.frags.insert(start_idx + 1, postlude)
                    }
                } else {
                    // We can't fit either.

                    if !merged {
                        self.frags.insert(start_idx, postlude)
                    }

                    self.frags.insert(start_idx, data);
                }
            },
            (Some(prelude), None) => {
                // See if we need to merge in the prelude.
                let start_idx = if start_idx > 0 {
                    if let InboundFrag::Needed {
                        offset: frag_offset,
                        len: frag_len,
                        ..
                    } = &mut self.frags[start_idx - 1] {
                        *frag_len = offset - *frag_offset;

                        start_idx
                    } else {
                        self.frags[start_idx] = prelude;

                        start_idx + 1
                    }
                } else {
                    self.frags[start_idx] = prelude;

                    start_idx + 1
                };

                if end_idx + 1 < self.frags.len() {
                    // See if the next fragment past the end is a data
                    // fragment.
                    if let InboundFrag::Data {
                        offset: frag_offset,
                        len: frag_len,
                        data: frag_data
                    } = &mut self.frags[end_idx + 1] {
                        // Merge in the new data.
                        *frag_offset = offset;
                        *frag_len += data_len;
                        frag_data.insert(0, data);

                        // Drop any intervening fragments.
                        if start_idx + 1 < end_idx {
                            let _ = self.frags
                                .drain(start_idx + 1..end_idx + 1);
                        }
                    } else {
                        self.add_data_frag_prelude(data, offset, data_len,
                                                   start_idx, end_idx)
                    }
                } else {
                    self.add_data_frag_prelude(data, offset, data_len,
                                               start_idx, end_idx)
                }
            }
            (None, Some(postlude)) => {
                // See if the first fragment is a data fragment.
                if let InboundFrag::Data {
                    len: frag_len,
                    data: frag_data,
                    ..
                } = &mut self.frags[start_idx] {
                    *frag_len += data_len;
                    frag_data.push(data);

                    // Drop any additional fragments.
                    if start_idx + 1 <= end_idx {
                        // We merged; we can just add the postlude.
                        self.frags[start_idx + 1] = postlude;

                        let _ = self.frags.drain(start_idx + 2..end_idx + 1);
                    } else {
                        self.frags.insert(start_idx + 1, postlude)
                    }
                } else {
                    // Replace the existing fragment with the data.
                    self.frags[start_idx] = InboundFrag::Data {
                        offset: offset,
                        len: data_len,
                        data: vec![data]
                    };

                    if end_idx + 1 < self.frags.len() {
                        if let InboundFrag::Needed {
                            offset: frag_offset,
                            len: frag_len,
                            ..
                        } = &mut self.frags[end_idx + 1] {
                            *frag_len = (*frag_offset + *frag_len) - (offset + data_len);
                            *frag_offset = offset + data_len;

                            // Drop any additional fragments.
                            if start_idx + 1 <= end_idx {
                                let _ = self.frags.drain(start_idx + 1..end_idx + 1);
                            }
                        } else if start_idx + 1 <= end_idx {
                            self.frags[start_idx + 1] = postlude;

                            // Drop any additional fragments.
                            if start_idx + 2 <= end_idx {
                                let _ = self.frags.drain(start_idx + 2..end_idx + 1);
                            }
                        } else {
                            self.frags.insert(start_idx + 1, postlude);
                        }
                    } else if start_idx + 1 <= end_idx {
                        self.frags[start_idx + 1] = postlude;

                        // Drop any additional fragments.
                        if start_idx + 2 <= end_idx {
                            let _ = self.frags.drain(start_idx + 2..end_idx + 1);
                        }
                    } else {
                        self.frags.insert(start_idx + 1, postlude);
                    }
                }
            }
            (None, None) => {
                // See if the first fragment is a data fragment.
                let left_merged = if let InboundFrag::Data {
                    offset: frag_offset,
                    data: frag_data,
                    len: frag_len
                } = &mut self.frags[start_idx] {
                    // Don't add the data if the fragment already
                    // subsumes it.
                    if offset < *frag_offset ||
                        data_end > *frag_offset + *frag_len {
                        *frag_len += data_len;
                        frag_data.push(data);
                    }

                    true
                } else {
                    self.try_right_merge_only(data, offset, data_len,
                                              start_idx, end_idx);

                    false
                };

                if left_merged {
                    // We've already merged on the left.

                    // Try to extract the data on the right.
                    let right_merge = if end_idx + 1 < self.frags.len() {
                        // See if the previous fragment is a data fragment.
                        if let InboundFrag::Data {
                            ..
                        } = &self.frags[end_idx + 1] {
                            // Drop all intervening fragments, and the
                            // adjacent one to the right.
                            match self.frags.drain(start_idx..end_idx + 2)
                                .last() {
                                Some(InboundFrag::Data {
                                    data: frag_data,
                                    len: frag_len,
                                    ..
                                }) => Some((frag_len, frag_data)),
                                _ => {
                                    // This should never happen.

                                    error!(target: "inbuond-frags",
                                           concat!("impossible case: ",
                                                   "right fragment should ",
                                                   "have been a data ",
                                                   "fragment"));

                                    None
                                }
                            }
                        } else {
                            // Drop all the intervening fragments.
                            let _ = self.frags.drain(start_idx..end_idx + 1);

                            None
                        }
                    } else {

                        // Drop all the intervening fragments.
                        let _ = self.frags.drain(start_idx + 1..end_idx + 1);

                        None
                    };

                    // Check if we need to merge the right-adjacent
                    // data into the left.
                    if let Some((right_len, mut frags)) = right_merge {
                        // Merge the data in.
                        if let InboundFrag::Data {
                            len: frag_len,
                            data: frag_data,
                            ..
                        } = &mut self.frags[end_idx + 1] {
                            // This should be the only case that ever happens
                            *frag_len += right_len;
                            frag_data.append(&mut frags);
                        } else {
                            // This should never happen.

                            error!(target: "inbuond-frags",
                                   concat!("impossible case: ",
                                           "left fragment should have ",
                                           "been a data fragment"));
                        }
                    }
                } else {
                println!("end_idx: {}", end_idx);
                }
            }
        }

        Ok(())
    }
}
/*
    fn split(
        self,
        data: Frag
    ) -> Option<(Option<Needed>, Frag, Option<Needed>)> {
        // Does the data start before self?
        if data.offset <= self.offset {
            // Does the data end after self?
            if data.offset + data.len >= self.offset + self.len {
                // The data completely replaces self.
                Some((None, data, None))
            } else {

            }

        } else
        }
    }
}

impl Frag {
    fn merge(
        mut self,
        mut other: Self
    ) -> Option<Frag> {
        if self.offset < other.offset &&
            self.offset + self.len == other.offset {
            // self is before other
            self.len += other.len;
            self.data.append(&mut other.data);

            Some(self)
        } else if other.offset < self.offset &&
            other.offset + other.len == self.offset {
            // other is before self
            other.len += self.len;
            other.data.append(&mut self.data);

            Some(other)
        } else {
            None
        }
    }
}
 */

impl PartialEq for InboundFrag {
    fn eq(
        &self,
        other: &Self
    ) -> bool {
        match (self, other) {
            (InboundFrag::Data { data: a_data, offset: a_offset, .. },
             InboundFrag::Data { data: b_data, offset: b_offset, .. }) =>
                a_offset == b_offset && a_data == b_data,
            (InboundFrag::Needed { offset: a_offset, len: a_len, .. },
             InboundFrag::Needed { offset: b_offset, len: b_len, .. }) =>
                a_offset == b_offset && a_len == b_len,
            _ => false
        }
    }
}

impl Eq for InboundFrag {}

impl Display for InboundInjectError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            InboundInjectError::OutOfBounds =>
                write!(f, "data extends beyond bounds")
        }
    }
}

#[test]
fn test_inject_needed() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4, 5, 6, 7]],
                offset: 0,
                len: 8
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4, 5, 6, 7]],
                offset: 3,
                len: 5
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4]],
                offset: 0,
                len: 3
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}


#[test]
fn test_inject_needed_pre_post() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4]],
                offset: 3,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            }
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8, 9, 10, 11]],
                offset: 4,
                len: 8
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8, 9, 10, 11]],
                offset: 7,
                len: 5
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8]],
                offset: 4,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8, 9, 10, 11]],
                offset: 7,
                len: 5
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8]
                ],
                offset: 0,
                len: 9
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4, 5, 6, 7]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4, 5, 6, 7]],
                offset: 3,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4]],
                offset: 0,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 7
            },
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4]],
                offset: 3,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 7
            },
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}
/*
#[test]
fn test_inject_needed_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3, 4, 5, 6, 7],
                    vec![8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![3, 4, 5, 6, 7],
                    vec![8, 9, 10, 11]
                ],
                offset: 3,
                len: 9
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4]],
                offset: 0,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4]],
                offset: 3,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8, 9, 10, 11]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8, 9, 10, 11]],
                offset: 7,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8]],
                offset: 4,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8, 9, 10, 11]],
                offset: 7,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![
                    vec![4, 5, 6, 7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 4,
                len: 12
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 7,
                len: 9
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8]],
                offset: 4,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 0,
                len: 16
            },
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 7,
                len: 9
            },
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8]
                ],
                offset: 0,
                len: 9
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}
*/
#[test]
fn test_inject_data() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_post() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre_post() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![7, 6, 5, 4, 3, 2, 1, 0]],
                offset: 0,
                len: 8
            }
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_post_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre_post_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}
/*
#[test]
fn test_inject_data_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_post_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre_post_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            },
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_data_pre_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_post_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_pre_post_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![11, 10, 9, 8, 7, 6, 5, 4]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}






#[test]
fn test_inject_needed_needed() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 16
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
                ],
                offset: 0,
                len: 16
            }
        ]
    };

    inbound.inject(
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        0
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 16
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
                ],
                offset: 3,
                len: 13
            }
        ]
    };

    inbound.inject(
        vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        3
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 16
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
                ],
                offset: 0,
                len: 13
            }
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            }
        ]
    };

    inbound.inject(
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        0
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}


#[test]
fn test_inject_needed_needed_pre_post() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 16
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
                ],
                offset: 3,
                len: 10
            }
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 13,
                len: 3
            }
        ]
    };

    inbound.inject(
        vec![3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        3
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![
                    vec![4, 5, 6, 7, 8, 9, 10, 11,
                         12, 13, 14, 15, 16, 17, 18, 19]
                ],
                offset: 4,
                len: 16
            }
        ]
    };

    inbound.inject(
        vec![4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        4
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
                ],
                offset: 7,
                len: 13
            }
        ]
    };

    inbound.inject(
        vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        7
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![
                    vec![4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                ],
                offset: 4,
                len: 13
            }
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 3
            }
        ]
    };

    inbound.inject(
        vec![4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        4
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_needed_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                ],
                offset: 7,
                len: 10
            }
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 3
            }
        ]
    };

    inbound.inject(
        vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        7
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11,
                         12, 13, 14, 15, 16, 17, 18, 19]
                ],
                offset: 0,
                len: 20
            }
        ]
    };

    inbound.inject(
        vec![4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        4
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
                ],
                offset: 7,
                len: 13
            }
        ]
    };

    inbound.inject(
        vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        7
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                ],
                offset: 0,
                len: 17
            }
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 3
            }
        ]
    };

    inbound.inject(
        vec![4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        4
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_data_before() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 8
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
                ],
                offset: 7,
                len: 10
            }
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 17,
                len: 3
            }
        ]
    };

    inbound.inject(
        vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        7
    ).expect("Expected success");

    assert_eq!(inbound, expected);
}




#[test]
fn test_inject_needed_needed_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4, 5, 6, 7]],
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4, 5, 6, 7]],
                offset: 3,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4]],
                offset: 0,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 7
            },
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4]],
                offset: 3,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 7
            },
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3, 4, 5, 6, 7],
                    vec![8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4, 5, 6, 7], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![3, 4, 5, 6, 7],
                    vec![8, 9, 10, 11]
                ],
                offset: 3,
                len: 9
            }
        ]
    };

    inbound.inject(vec![3, 4, 5, 6, 7], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3, 4]],
                offset: 0,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![0, 1, 2, 3, 4], 0).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![3, 4]],
                offset: 3,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 5,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![8, 9, 10, 11]],
                offset: 8,
                len: 4
            }
        ]
    };

    inbound.inject(vec![3, 4], 3).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8, 9, 10, 11]],
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8, 9, 10, 11]],
                offset: 7,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8]],
                offset: 4,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_needed_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8, 9, 10, 11]],
                offset: 7,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11]
                ],
                offset: 0,
                len: 12
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_data_before_needed_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 7
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![
                    vec![4, 5, 6, 7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 4,
                len: 12
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 7,
                len: 9
            }
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Data {
                data: vec![vec![4, 5, 6, 7, 8]],
                offset: 4,
                len: 5
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_needed_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 7
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 0,
                len: 16
            },
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8, 9, 10, 11], 4)
        .expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![
                    vec![7, 8, 9, 10, 11],
                    vec![12, 13, 14, 15]
                ],
                offset: 7,
                len: 9
            },
        ]
    };

    inbound.inject(vec![7, 8, 9, 10, 11], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_post_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![
                    vec![0, 1, 2, 3],
                    vec![4, 5, 6, 7, 8]
                ],
                offset: 0,
                len: 9
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![4, 5, 6, 7, 8], 4).expect("Expected success");

    assert_eq!(inbound, expected);
}

#[test]
fn test_inject_needed_needed_pre_post_data_before_data_after() {
    let mut inbound = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 8
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };
    let expected = InboundFrags {
        frags: vec![
            InboundFrag::Data {
                data: vec![vec![0, 1, 2, 3]],
                offset: 0,
                len: 4
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 4,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![7, 8]],
                offset: 7,
                len: 2
            },
            InboundFrag::Needed {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 3
            },
            InboundFrag::Data {
                data: vec![vec![12, 13, 14, 15]],
                offset: 12,
                len: 4
            }
        ]
    };

    inbound.inject(vec![7, 8], 7).expect("Expected success");

    assert_eq!(inbound, expected);
}
*/
