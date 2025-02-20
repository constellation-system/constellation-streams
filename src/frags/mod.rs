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
struct Frag {
    nretries: usize,
    when: Instant,
    offset: usize,
    len: usize
}

#[derive(Debug, Eq, PartialEq)]
pub struct Frags {
    // XXX use a good data structure here, like a splay tree.
    frags: Vec<Frag>
}

#[derive(Debug)]
pub enum InboundInjectError {
    OutOfBounds
}

#[derive(Debug)]
pub enum OutboundAckError {
    OutOfBounds
}

impl Frags {
    /// Record an acknowledgement of a range of data.
    ///
    /// This will remove the given range from the active ranges that
    /// need to be transmitted.
    pub fn remove(
        &mut self,
        offset: usize,
        len: usize
    ) {
        let nfrags = self.frags.len();
        let ack_end = offset + len;
        let start_idx = match self.frags
            .binary_search_by(|frag| frag.offset.cmp(&offset)) {
            Ok(idx) => idx,
            Err(idx) => if idx != 0 {
                idx - 1
            } else {
                idx
            }
        };

        // We won't need to do anything if we're already beyond the
        // end of the fragments.
        if start_idx < nfrags {
            let end_idx = match self.frags.binary_search_by(|frag| {
                let frag_end = frag.offset + frag.len;

                frag_end.cmp(&ack_end)
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
                        nretries: 0,
                    };

                    start_idx + 1
                } else {
                    start_idx
                };

                // See if we need a postlude.
                if end_offset + end_len != ack_end {
                    let ack_end = (offset + len) - end_offset;
                    let postlude = Frag {
                        offset: end_offset + ack_end,
                        len: end_len - ack_end,
                        when: Instant::now(),
                        nretries: 0,
                    };

                    // See if we can add reuse an existing fragment.
                    if start_idx <= end_idx {
                        // Check that the ack range doesn't end at the
                        // end fragment's start.
                        if end_offset < offset + len {
                            // Use the first fragment.
                            self.frags[start_idx] = postlude;

                            // Delete the remaining fragments.
                            if start_idx + 1 <= end_idx {
                                let _ = self.frags
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
                        nretries: 0,
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

impl PartialEq for Frag {
    fn eq(
        &self,
        other: &Self
    ) -> bool {
        self.offset == other.offset && self.len == other.len
    }
}

impl Eq for Frag {}

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

impl Display for OutboundAckError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            OutboundAckError::OutOfBounds =>
                write!(f, "acknowledgement extends beyond bounds")
        }
    }
}

#[test]
fn test_frags_remove_single_miss_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            }
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_miss_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };

    frags.remove(8, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_exact() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_space_left_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

    frags.remove(0, 10);

    assert_eq!(frags, expected);
}


#[test]
fn test_frags_remove_single_pre() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            }
        ]
    };

    frags.remove(1, 7);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_space_right() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            }
        ]
    };

    frags.remove(1, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 9
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            }
        ]
    };

    frags.remove(0, 8);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_post_space_left() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 1,
                len: 9
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            }
        ]
    };

    frags.remove(0, 9);

    assert_eq!(frags, expected);
}

#[test]
fn test_frags_remove_single_pre_post() {
    let mut frags = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 10
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            }
        ]
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            }
        ]
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            }
        ]
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![]
    };

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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 1
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 1
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 1
            }
        ]
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 0,
                len: 8
            }
        ]
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 8,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 9,
                len: 8
            }
        ]
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
            }
        ]
    };
    let expected = Frags {
        frags: vec![
            Frag {
                when: Instant::now(),
                nretries: 0,
                offset: 10,
                len: 8
            }
        ]
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
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
            }
        ]
    };

    frags.remove(9, 8);

    assert_eq!(frags, expected);
}
