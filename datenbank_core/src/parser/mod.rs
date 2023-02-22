use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1};
use nom::combinator::recognize;
use nom::multi::many0;
use nom::sequence::pair;
use nom::{Finish, IResult};

use create_table::create_table;
use insert_into::insert_into;

mod create_table;
mod insert_into;
mod literal;

#[derive(Debug, Clone, thiserror::Error, PartialEq)]
#[error("error parsing SQL input: {msg}")]
pub struct Error {
    // TODO: its not ideal to drop the parser error info here, but its being stupid
    msg: String,
}

// Input is the parsed abstract syntax tree for valid SQL input.
#[derive(Debug, PartialEq)]
pub enum Input<'a> {
    Create {
        table_name: &'a str,
        schema: Vec<ColumnSchema<'a>>,
    },
    InsertInto {
        table_name: &'a str,
        columns: Vec<&'a str>,
        values: Vec<Vec<Literal>>,
    },
}

// The ColumnType and ColumnSchema info is effectively identical to what is in the schema package,
// but I'm duplicating here so we can separate the act of parsing from the domain-specific
// validation of the schema itself.

#[derive(Debug, PartialEq)]
pub enum ColumnType {
    VarChar(u16),
    Int,
    Bool,
}

#[derive(Debug, PartialEq)]
pub enum Literal {
    String(String),
    Int(i32),
    Bool(bool),
}

#[derive(Debug, PartialEq)]
pub struct ColumnSchema<'a> {
    pub column_name: &'a str,
    pub column_type: ColumnType,
}

impl<'a> ColumnSchema<'a> {
    fn new(column_name: &'a str, column_type: ColumnType) -> Self {
        ColumnSchema {
            column_name,
            column_type,
        }
    }
}

pub fn parse(input_str: &str) -> Result<Input, Error> {
    let parse_result = alt((create_table, insert_into))(input_str);
    match parse_result.finish() {
        Ok((_, input)) => Ok(input),
        Err(err) => Err(Error {
            msg: err.to_string(),
        }),
    }
}

// identifiers start with an alphabetic character and can contain alphanumeric and _ as following
// characters.
fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(alpha1, many0(alt((alphanumeric1, tag("_"))))))(input)
}

// this is just a test util that can be shared across all parser testing
#[cfg(test)]
fn parse_check<'a, T, F>(f: F, input: &'a str, expected: Option<T>)
where
    T: std::fmt::Debug + PartialEq,
    F: Fn(&'a str) -> IResult<&'a str, T>,
{
    match (f(input), expected) {
        (Ok((rest, result)), Some(exp)) => {
            assert!(rest.is_empty());
            assert_eq!(result, exp);
        }
        (Err(_), None) => (),
        (Ok(_), None) => panic!("shouldn't have passed"),
        (Err(err), Some(_)) => panic!("should't have errored: {}", err),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_identifier() {
        fn check(input: &str, expected: Option<&str>) {
            parse_check(identifier, input, expected)
        }

        check("1foo", None);
        check("Foo", Some("Foo"));
        check("Foo1", Some("Foo1"));
        check("foo_bar", Some("foo_bar"));
        check("Foo_Bar", Some("Foo_Bar"));
        check("Foo_1", Some("Foo_1"));
        check("foo_bar_baz", Some("foo_bar_baz"));
        check("foo_2_baz", Some("foo_2_baz"));
        check("_must_not_start", None);
    }
}
