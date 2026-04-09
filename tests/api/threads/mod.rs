// Copyright © 2024-26 The Johns Hopkins Applied Physics Laboratory LLC.
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

use constellation_streams::threads::Tokens;
use constellation_streams::threads::TokensCtx;

#[cfg(test)]
use crate::init;

mod private;
mod shared;

#[test]
fn test_tokens_alloc_free_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let expected = tokens.token();

    tokens.free_token(expected.clone());

    let actual = tokens.token();

    assert_eq!(expected, actual)
}

#[test]
fn test_tokens_unique() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();

    assert_ne!(first, second)
}

#[test]
fn test_tokens_alloc_free_first_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();

    tokens.free_token(first);

    let third = tokens.token();

    assert_ne!(second, third)
}

#[test]
fn test_tokens_alloc_free_second_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let _ = tokens.token();
    let second = tokens.token();

    tokens.free_token(second.clone());

    let third = tokens.token();

    assert_eq!(second, third)
}

#[test]
fn test_tokens_alloc_free_all_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let third = tokens.token();

    tokens.free_token(first.clone());
    tokens.free_token(second);
    tokens.free_token(third);

    let fourth = tokens.token();

    assert_eq!(first, fourth)
}

#[test]
fn test_tokens_alloc_free_all_rev_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let third = tokens.token();

    tokens.free_token(third);
    tokens.free_token(second);
    tokens.free_token(first.clone());

    let fourth = tokens.token();

    assert_eq!(first, fourth)
}

#[test]
fn test_tokens_alloc_free_gap_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let _ = tokens.token();
    let fourth = tokens.token();

    tokens.free_token(first);
    tokens.free_token(second);
    tokens.free_token(fourth.clone());

    let fifth = tokens.token();

    let sixth = tokens.token();

    assert_eq!(second, fifth);
    assert_eq!(first, sixth)
}

#[test]
fn test_tokens_alloc_free_close_gap_alloc() {
    init();

    let mut tokens = Tokens::with_capacity(1);
    let first = tokens.token();
    let second = tokens.token();
    let third = tokens.token();
    let fourth = tokens.token();

    tokens.free_token(first.clone());
    tokens.free_token(second);
    tokens.free_token(fourth);
    tokens.free_token(third);

    let fifth = tokens.token();

    assert_eq!(fifth, first)
}
