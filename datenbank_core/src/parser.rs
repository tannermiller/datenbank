use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{
    alpha1, alphanumeric1, char, multispace0, newline, space0, space1, u16 as char_u16,
};
use nom::combinator::recognize;
use nom::multi::{many0, many1};
use nom::sequence::{delimited, pair, separated_pair, tuple};
use nom::{Finish, IResult};

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

fn create_table(input: &str) -> IResult<&str, Input> {
    let (input, table_name) = create_table_header(input)?;
    let (input, schema) = many1(column_schema)(input)?;
    let (input, _) = pair(char('}'), multispace0)(input)?;
    Ok((input, Input::Create { table_name, schema }))
}

// parse "CREATE TABLE tablename {"
fn create_table_header(input: &str) -> IResult<&str, &str> {
    delimited(
        pair(tag_no_case("create table"), space1),
        identifier,
        tuple((space1, char('{'), newline)),
    )(input)
}

fn column_type_varchar(input: &str) -> IResult<&str, ColumnType> {
    pair(
        tag_no_case("varchar"),
        delimited(char('('), char_u16, char(')')),
    )(input)
    .map(|(rest, (_, size))| (rest, ColumnType::VarChar(size)))
}

fn column_type_int(input: &str) -> IResult<&str, ColumnType> {
    tag_no_case("int")(input).map(|(rest, _)| (rest, ColumnType::Int))
}

fn column_type_bool(input: &str) -> IResult<&str, ColumnType> {
    tag_no_case("bool")(input).map(|(rest, _)| (rest, ColumnType::Bool))
}

fn column_type(input: &str) -> IResult<&str, ColumnType> {
    alt((column_type_varchar, column_type_int, column_type_bool))(input)
}

fn column_schema(input: &str) -> IResult<&str, ColumnSchema> {
    delimited(
        space0,
        separated_pair(identifier, space1, column_type),
        newline,
    )(input)
    .map(|(rest, (column_name, column_type))| (rest, ColumnSchema::new(column_name, column_type)))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_create_table_header() {
        fn check(input: &str, expected: Option<&str>) {
            match (create_table_header(input), expected) {
                (Ok((rest, result)), Some(exp)) => {
                    assert!(rest.is_empty());
                    assert_eq!(result, exp);
                }
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        check("CREATE TABLE foo {\n", Some("foo"));
        check("CREATE TABLE Foo {\n", Some("Foo"));
        check("CREATE TABLE FOO {\n", Some("FOO"));
        check("create table FOO {\n", Some("FOO"));
        check("Create Table FOO {\n", Some("FOO"));
        check("Craete Table foo {\n", None);
        check("Create Table foo {", None);
        check("definitely not", None);
        check(" Create Table foo {", None);
    }

    #[test]
    fn test_identifier() {
        fn check(input: &str, expected: Option<&str>) {
            match (identifier(input), expected) {
                (Ok((rest, result)), Some(exp)) => {
                    assert!(rest.is_empty());
                    assert_eq!(result, exp);
                }
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
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
    fn test_column_type() {
        fn check(input: &str, expected: Option<ColumnType>) {
            match (column_type(input), expected) {
                (Ok((rest, result)), Some(exp)) => {
                    assert!(rest.is_empty());
                    assert_eq!(result, exp);
                }
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        check("nope", None);
        check("int", Some(ColumnType::Int));
        check("Int", Some(ColumnType::Int));
        check("INT", Some(ColumnType::Int));
        check("bool", Some(ColumnType::Bool));
        check("Bool", Some(ColumnType::Bool));
        check("BOOL", Some(ColumnType::Bool));
        check("varchar(1)", Some(ColumnType::VarChar(1)));
        check("VarChar(1)", Some(ColumnType::VarChar(1)));
        check("VarChar()", None);
        check("VarChar(840)", Some(ColumnType::VarChar(840)));
        check("VarChar(10000000000000000000000)", None);
    }

    #[test]
    fn test_column_schema() {
        fn check(input: &str, expected: Option<ColumnSchema>) {
            match (column_schema(input), expected) {
                (Ok((rest, result)), Some(exp)) => {
                    assert!(rest.is_empty());
                    assert_eq!(result, exp);
                }
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        check("nope", None);
        check(
            "some int\n",
            Some(ColumnSchema::new("some", ColumnType::Int)),
        );
        check(
            "  some int\n",
            Some(ColumnSchema::new("some", ColumnType::Int)),
        );
        check(
            "\tsome int\n",
            Some(ColumnSchema::new("some", ColumnType::Int)),
        );
        check("\tsome nope\n", None);
        check("some int", None);
        check(
            "\tsome      \t     int\n",
            Some(ColumnSchema::new("some", ColumnType::Int)),
        );
    }

    #[test]
    fn test_create_table() {
        fn check(input: &str, expected: Option<Input>) {
            match (create_table(input), expected) {
                (Ok((rest, result)), Some(exp)) => {
                    assert!(rest.is_empty());
                    assert_eq!(result, exp);
                }
                (Err(_), None) => (),
                (Ok(_), None) => panic!("shouldn't have passed"),
                (Err(err), Some(_)) => panic!("should't have errored: {}", err),
            }
        }

        check("nope", None);
        check(
            r"CREATE TABLE Foo {
    SomeInt INT
  some_bool Bool
VC82 VarChar(82)
}
",
            Some(Input::Create {
                table_name: "Foo",
                schema: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                ],
            }),
        );

        check(
            r"CREATE TABLE Foo {
    SomeInt INT
    some_bool Bool
    VC82 VarChar(82)
}",
            Some(Input::Create {
                table_name: "Foo",
                schema: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                ],
            }),
        );
    }
}
