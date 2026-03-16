use constellation_streams::threads::Tokens;
use constellation_streams::threads::TokensCtx;

#[cfg(test)]
use crate::init;

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
