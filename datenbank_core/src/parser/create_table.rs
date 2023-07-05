use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{
    char, multispace0, multispace1, newline, space0, space1, u16 as char_u16, u32 as char_u32,
};
use nom::combinator::all_consuming;
use nom::combinator::opt;
use nom::multi::{many0, many1};
use nom::sequence::{delimited, pair, separated_pair, tuple};
use nom::IResult;

use super::{identifier, ColumnSchema, ColumnType, Index, Input};

pub fn create_table(input: &str) -> IResult<&str, Input> {
    let (input, (table_name, columns, primary_key, indices, _)) = all_consuming(tuple((
        create_table_header,
        many1(column_schema),
        opt(primary_key),
        many0(index),
        pair(char('}'), multispace0),
    )))(input)?;
    Ok((
        input,
        Input::Create {
            table_name,
            columns,
            primary_key,
            indices,
        },
    ))
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

fn column_type_longblob(input: &str) -> IResult<&str, ColumnType> {
    pair(
        tag_no_case("longblob"),
        delimited(char('('), char_u32, char(')')),
    )(input)
    .map(|(rest, (_, size))| (rest, ColumnType::LongBlob(size)))
}

fn column_type(input: &str) -> IResult<&str, ColumnType> {
    alt((
        column_type_varchar,
        column_type_int,
        column_type_bool,
        column_type_longblob,
    ))(input)
}

fn column_schema(input: &str) -> IResult<&str, ColumnSchema> {
    delimited(
        space0,
        separated_pair(identifier, space1, column_type),
        newline,
    )(input)
    .map(|(rest, (column_name, column_type))| (rest, ColumnSchema::new(column_name, column_type)))
}

fn primary_key(input: &str) -> IResult<&str, Vec<&str>> {
    let (input, (_, _, _, cols)) = tuple((
        space0,
        tag_no_case("primary key"),
        multispace1,
        index_column_list,
    ))(input)?;

    Ok((input, cols))
}

fn index(input: &str) -> IResult<&str, Index> {
    let (input, (_, _, _, name, _, columns)) = tuple((
        space0,
        tag_no_case("index"),
        multispace1,
        identifier,
        multispace1,
        index_column_list,
    ))(input)?;

    Ok((input, Index { name, columns }))
}

fn index_column_list(input: &str) -> IResult<&str, Vec<&str>> {
    let (input, (first, other_cols)) = delimited(
        tuple((tag("("), multispace0)),
        pair(
            identifier,
            many0(tuple((tag(","), multispace1, identifier))),
        ),
        tuple((multispace0, tag(")"), newline)),
    )(input)?;

    let mut cols = Vec::with_capacity(other_cols.len() + 1);
    cols.push(first);
    cols.extend(other_cols.into_iter().map(|(_, _, col)| col));
    Ok((input, cols))
}

#[cfg(test)]
mod test {
    use super::super::parse_check;
    use super::*;

    #[test]
    fn test_create_table_header() {
        fn check(input: &str, expected: Option<&str>) {
            parse_check(create_table_header, input, expected)
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
    fn test_column_type() {
        fn check(input: &str, expected: Option<ColumnType>) {
            parse_check(column_type, input, expected)
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
        check("longblob(1)", Some(ColumnType::LongBlob(1)));
        check("LongBlob(1)", Some(ColumnType::LongBlob(1)));
        check("LongBlob()", None);
        check("LongBlob(66000)", Some(ColumnType::LongBlob(66000)));
        check("LongBlob(10000000000000000000000)", None);
    }

    #[test]
    fn test_column_schema() {
        fn check(input: &str, expected: Option<ColumnSchema>) {
            parse_check(column_schema, input, expected)
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
    fn test_primary_key() {
        fn check(input: &str, expected: Option<Vec<&str>>) {
            parse_check(primary_key, input, expected)
        }

        check("nope", None);
        check("", None);
        check("PRIMARY KEY (foo)\n", Some(vec!["foo"]));
        check("PRIMARY KEY (foo, bar)\n", Some(vec!["foo", "bar"]));
        check("PRIMARY KEY (\nfoo,\nbar\n)\n", Some(vec!["foo", "bar"]));
    }

    #[test]
    fn test_index() {
        fn check(input: &str, expected: Option<Index>) {
            parse_check(index, input, expected)
        }

        check("nope", None);
        check("", None);
        check(
            "INDEX idx_foo (foo)\n",
            Some(Index {
                name: "idx_foo",
                columns: vec!["foo"],
            }),
        );
        check(
            "INDEX IdxFooBar (foo, bar)\n",
            Some(Index {
                name: "IdxFooBar",
                columns: vec!["foo", "bar"],
            }),
        );
        check(
            "INDEX whats_an_INDEX (\nfoo,\nbar\n)\n",
            Some(Index {
                name: "whats_an_INDEX",
                columns: vec!["foo", "bar"],
            }),
        );
    }

    #[test]
    fn test_create_table() {
        fn check(input: &str, expected: Option<Input>) {
            parse_check(create_table, input, expected)
        }

        check("nope", None);
        check(
            r"CREATE TABLE Foo {
    SomeInt INT
  some_bool Bool
VC82 VarChar(82)
 lonG_Blob_big LONGblob(100000)
}
",
            Some(Input::Create {
                table_name: "Foo",
                columns: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                    ColumnSchema::new("lonG_Blob_big", ColumnType::LongBlob(100000)),
                ],
                primary_key: None,
                indices: vec![],
            }),
        );

        check(
            r"CREATE TABLE Foo {
    SomeInt INT
    some_bool Bool
    VC82 VarChar(82)
    lonG_Blob_big LONGblob(100000)
}",
            Some(Input::Create {
                table_name: "Foo",
                columns: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                    ColumnSchema::new("lonG_Blob_big", ColumnType::LongBlob(100000)),
                ],
                primary_key: None,
                indices: vec![],
            }),
        );

        check(
            r"CREATE TABLE Foo {
    SomeInt INT
    some_bool Bool
    VC82 VarChar(82)
    lonG_Blob_big LONGblob(100000)
    PRIMARY KEY (SomeInt)
}",
            Some(Input::Create {
                table_name: "Foo",
                columns: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                    ColumnSchema::new("lonG_Blob_big", ColumnType::LongBlob(100000)),
                ],
                primary_key: Some(vec!["SomeInt"]),
                indices: vec![],
            }),
        );

        check(
            r"CREATE TABLE Foo {
    SomeInt INT
    some_bool Bool
    VC82 VarChar(82)
    lonG_Blob_big LONGblob(100000)
    PRIMARY KEY (
        SomeInt,
        some_bool,
        VC82,
        lonG_Blob_big
    )
}",
            Some(Input::Create {
                table_name: "Foo",
                columns: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                    ColumnSchema::new("lonG_Blob_big", ColumnType::LongBlob(100000)),
                ],
                primary_key: Some(vec!["SomeInt", "some_bool", "VC82", "lonG_Blob_big"]),
                indices: vec![],
            }),
        );

        check(
            r"CREATE TABLE Foo {
    SomeInt INT
    some_bool Bool
    VC82 VarChar(82)
    INDEX idx_SomeInt_some_bool_VC82 (
        SomeInt,
        some_bool,
        VC82
    )
}",
            Some(Input::Create {
                table_name: "Foo",
                columns: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                ],
                primary_key: None,
                indices: vec![Index {
                    name: "idx_SomeInt_some_bool_VC82",
                    columns: vec!["SomeInt", "some_bool", "VC82"],
                }],
            }),
        );

        check(
            r"CREATE TABLE Foo {
    SomeInt INT
    some_bool Bool
    VC82 VarChar(82)
    PRIMARY KEY (SomeInt, some_bool, VC82)
    INDEX loveIndices (VC82, SomeInt)
    INDEX also_love_INDEX42 (some_bool, SomeInt)
}",
            Some(Input::Create {
                table_name: "Foo",
                columns: vec![
                    ColumnSchema::new("SomeInt", ColumnType::Int),
                    ColumnSchema::new("some_bool", ColumnType::Bool),
                    ColumnSchema::new("VC82", ColumnType::VarChar(82)),
                ],
                primary_key: Some(vec!["SomeInt", "some_bool", "VC82"]),
                indices: vec![
                    Index {
                        name: "loveIndices",
                        columns: vec!["VC82", "SomeInt"],
                    },
                    Index {
                        name: "also_love_INDEX42",
                        columns: vec!["some_bool", "SomeInt"],
                    },
                ],
            }),
        );
    }
}
