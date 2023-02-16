use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, alphanumeric1, char, multispace0, multispace1, space0};
use nom::combinator::recognize;
use nom::multi::many0;
use nom::sequence::{delimited, pair};
use nom::{Finish, IResult};

use create_table::create_table;

mod create_table;

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
    match create_table(input_str).finish() {
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

