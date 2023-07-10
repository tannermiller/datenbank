use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{alpha1, alphanumeric1};
use nom::combinator::recognize;
use nom::multi::many0;
use nom::sequence::pair;
use nom::{Finish, IResult};

use create_table::create_table;
use insert_into::insert_into;
use select_from::select_from;

mod create_table;
mod insert_into;
mod literal;
mod select_from;

#[derive(Debug, Clone, thiserror::Error, PartialEq)]
#[error("error parsing SQL input: {msg}")]
pub struct Error {
    // TODO: its not ideal to drop the parser error info here, but its being stupid
    pub msg: String,
}

// Input is the parsed abstract syntax tree for valid SQL input.
#[derive(Debug, PartialEq)]
pub enum Input<'a> {
    Create {
        table_name: &'a str,
        columns: Vec<ColumnSchema<'a>>,
        primary_key: Option<Vec<&'a str>>,
    },
    InsertInto {
        table_name: &'a str,
        columns: Vec<&'a str>,
        values: Vec<Vec<Literal>>,
    },
    SelectFrom {
        table_name: &'a str,
        columns: SelectColumns<'a>,
        where_clause: Option<Expression<'a>>,
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
    LongBlob(u32),
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Literal {
    String(String),
    Int(i32),
    Bool(bool),
    Bytes(Vec<u8>),
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Literal::String(s) => write!(f, "\"{s}\""),
            Literal::Int(i) => i.fmt(f),
            Literal::Bool(b) => write!(f, "{}", b.to_string().to_uppercase()),
            Literal::Bytes(bs) => {
                write!(f, "X'")?;
                for b in bs {
                    write!(f, "{:02X}", b)?;
                }
                write!(f, "'")
            }
        }
    }
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

#[derive(Debug, PartialEq)]
pub enum SelectColumns<'a> {
    Star,
    Explicit(Vec<&'a str>),
}

pub fn parse(input_str: &str) -> Result<Input, Error> {
    let parse_result = alt((create_table, insert_into, select_from))(input_str);
    match parse_result.finish() {
        Ok((_, input)) => Ok(input),
        Err(err) => Err(Error {
            msg: err.to_string(),
        }),
    }
}

// identifier and identifier_bytes are identical other than their type signatures. I've so far
// failed to make them generic due to the large number of types that nom operates on that are
// seemingly handled differently for &str and &[u8].

// identifiers start with an alphabetic character and can contain alphanumeric and _ as following
// characters.
pub fn identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(alpha1, many0(alt((alphanumeric1, tag("_"))))))(input)
}

// identifiers start with an alphabetic character and can contain alphanumeric and _ as following
// characters.
pub fn identifier_bytes(input: &[u8]) -> IResult<&[u8], &[u8]> {
    recognize(pair(alpha1, many0(alt((alphanumeric1, tag("_"))))))(input)
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum EqualityOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqualTo,
    LessThan,
    LessThanOrEqualTo,
}

impl std::fmt::Display for EqualityOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sign = match self {
            EqualityOp::Equal => "=",
            EqualityOp::NotEqual => "!=",
            EqualityOp::GreaterThan => ">",
            EqualityOp::GreaterThanOrEqualTo => ">=",
            EqualityOp::LessThan => "<",
            EqualityOp::LessThanOrEqualTo => "<=",
        };
        write!(f, "{}", sign)
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
}

impl std::fmt::Display for LogicalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sign = match self {
            LogicalOp::And => "AND",
            LogicalOp::Or => "OR",
        };
        write!(f, "{}", sign)
    }
}

#[derive(Debug, PartialEq)]
pub enum Expression<'a> {
    Comparison(Terminal<'a>, EqualityOp, Terminal<'a>),
    Logical(Box<Expression<'a>>, LogicalOp, Box<Expression<'a>>),
}

#[derive(Debug, PartialEq)]
pub enum Terminal<'a> {
    Identifier(&'a str),
    Literal(Literal),
}

fn hex_string_to_bytes(s: &str) -> Result<Vec<u8>, Error> {
    if s.len() < 3 || &s[..2] != "X'" || &s[(s.len() - 1)..] != "'" {
        return Err(Error {
            msg: "hex literal needs to start with \"X'\" and end with \"'\"".into(),
        });
    }

    // slice out of the delimiters
    let hex_str = &s[2..(s.len() - 1)];

    if hex_str.len() % 2 != 0 {
        return Err(Error {
            msg: "hex literal must have an even number of chars".into(),
        });
    };

    hex_str
        .chars()
        .enumerate()
        .step_by(2)
        .map(|(i, _)| u8::from_str_radix(&hex_str[i..i + 2], 16))
        .collect::<Result<Vec<u8>, std::num::ParseIntError>>()
        .map_err(|e| Error { msg: e.to_string() })
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
        (Ok(_), None) => panic!("input \"{input}\" shouldn't have passed"),
        (Err(err), Some(_)) => panic!("input \"{input}\" should't have errored: {err}"),
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

    #[test]
    fn test_literal_bytes_to_string() {
        assert_eq!(
            "X'2A0164'".to_string(),
            Literal::Bytes(vec![42, 1, 100]).to_string(),
        );
    }

    #[test]
    fn test_hex_string_to_bytes() {
        assert_eq!(vec![42, 1, 100], hex_string_to_bytes("X'2A0164'").unwrap());
    }

    #[test]
    fn test_bytes_to_hex_reflective() {
        let input = "X'2A0164'";
        assert_eq!(
            input,
            Literal::Bytes(hex_string_to_bytes(input).unwrap()).to_string(),
        );
    }
}
