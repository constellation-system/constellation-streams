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
    pub fn insert(
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
    pub fn remove(
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
            InboundInjectError::OutOfBounds => {
                write!(f, "data extends beyond bounds")
            }
        }
    }
}

impl Display for OutboundAckError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            OutboundAckError::OutOfBounds => {
                write!(f, "acknowledgement extends beyond bounds")
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
