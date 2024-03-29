use nom::bytes::complete::tag_no_case;
use nom::character::complete::{char, multispace0, multispace1, space1};
use nom::combinator::all_consuming;
use nom::multi::many0;
use nom::sequence::{delimited, pair, tuple};
use nom::IResult;

use super::literal::literal;
use super::{identifier, Input, Literal};

// parse a sql statement like "INSERT INTO foo (col1, col2) VALUES (val1, val2), (val3, val4)"
pub fn insert_into(input: &str) -> IResult<&str, Input> {
    let (rest, (table_name, _, columns, _, values)) = all_consuming(tuple((
        prefix_with_table,
        multispace1,
        column_names,
        pair(tag_no_case("values"), multispace1),
        values_multi,
    )))(input)?;
    Ok((
        rest,
        Input::InsertInto {
            table_name,
            columns,
            values,
        },
    ))
}

fn prefix_with_table(input: &str) -> IResult<&str, &str> {
    let (rest, (_, _, table_name)) =
        tuple((tag_no_case("insert into"), multispace1, identifier))(input)?;
    Ok((rest, table_name))
}

fn column_names(input: &str) -> IResult<&str, Vec<&str>> {
    delimited(
        pair(char('('), multispace0),
        column_names_multi,
        tuple((multispace0, char(')'), multispace1)),
    )(input)
}

fn column_names_multi(input: &str) -> IResult<&str, Vec<&str>> {
    let (rest, (mut values, final_val)) = pair(column_names_multi_comma, identifier)(input)?;
    values.push(final_val);
    Ok((rest, values))
}

fn column_names_multi_comma(input: &str) -> IResult<&str, Vec<&str>> {
    let (rest, idents) = many0(tuple((identifier, char(','), multispace1)))(input)?;
    Ok((
        rest,
        idents.into_iter().map(|(ident, _, _)| ident).collect(),
    ))
}

fn values_multi(input: &str) -> IResult<&str, Vec<Vec<Literal>>> {
    let (rest, (first_val, rest_vals)) = pair(
        values_single,
        many0(tuple((char(','), multispace1, values_single))),
    )(input)?;
    let mut values = vec![first_val];
    for (_, _, val) in rest_vals {
        values.push(val);
    }
    Ok((rest, values))
}

fn values_single(input: &str) -> IResult<&str, Vec<Literal>> {
    let (rest, (first_val, rest_vals)) = delimited(
        char('('),
        pair(literal, many0(tuple((char(','), space1, literal)))),
        char(')'),
    )(input)?;
    let mut values = vec![first_val];
    for (_, _, val) in rest_vals {
        values.push(val);
    }
    Ok((rest, values))
}

#[cfg(test)]
mod test {
    use super::super::parse_check;
    use super::*;

    #[test]
    fn test_prefix_with_table() {
        fn check(input: &str, expected: Option<&str>) {
            parse_check(prefix_with_table, input, expected)
        }

        check("nope", None);
        check("insert into foo", Some("foo"));
        check("insert into foo", Some("foo"));
    }

    #[test]
    fn test_column_names_multi_comma() {
        fn check(input: &str, expected: Option<Vec<&str>>) {
            parse_check(column_names_multi_comma, input, expected)
        }

        check("foo, bar, ", Some(vec!["foo", "bar"]));
        check("foo,\nbar,\n", Some(vec!["foo", "bar"]));
    }

    #[test]
    fn test_column_names_multi() {
        fn check(input: &str, expected: Option<Vec<&str>>) {
            parse_check(column_names_multi, input, expected)
        }

        check("foo, bar, baz", Some(vec!["foo", "bar", "baz"]));
        check("foo, bar", Some(vec!["foo", "bar"]));
    }

    #[test]
    fn test_column_names() {
        fn check(input: &str, expected: Option<Vec<&str>>) {
            parse_check(column_names, input, expected)
        }

        check("nope", None);
        check("(foo", None);
        check("(foo) ", Some(vec!["foo"]));
        check("(foo, bar)\n", Some(vec!["foo", "bar"]));
        check("(foo, bar\n   ) ", Some(vec!["foo", "bar"]));
        check("( foo, bar, baz ) ", Some(vec!["foo", "bar", "baz"]));
    }

    #[test]
    fn test_values_single() {
        fn check(input: &str, expected: Option<Vec<Literal>>) {
            parse_check(values_single, input, expected)
        }

        check("nope", None);
        check("(7)", Some(vec![Literal::Int(7)]));
        check("(true)", Some(vec![Literal::Bool(true)]));
        check(
            "(\"\\\"Hello, World!\\\"\")",
            Some(vec![Literal::String("\"Hello, World!\"".to_string())]),
        );
        check(
            "(7, FALSE)",
            Some(vec![Literal::Int(7), Literal::Bool(false)]),
        );
        check(
            "(\"something\", 42)",
            Some(vec![
                Literal::String("something".to_string()),
                Literal::Int(42),
            ]),
        );
        check(
            "(42, \"something\")",
            Some(vec![
                Literal::Int(42),
                Literal::String("something".to_string()),
            ]),
        );
        check(
            "(7, FALSE, \"are you ready?\")",
            Some(vec![
                Literal::Int(7),
                Literal::Bool(false),
                Literal::String("are you ready?".to_string()),
            ]),
        );
    }

    #[test]
    fn test_values_multi() {
        fn check(input: &str, expected: Option<Vec<Vec<Literal>>>) {
            parse_check(values_multi, input, expected)
        }

        check("nope", None);
        check("(7)", Some(vec![vec![Literal::Int(7)]]));
        check(
            "(7), (8)",
            Some(vec![vec![Literal::Int(7)], vec![Literal::Int(8)]]),
        );
        check(
            "(7),\n  (8), (\"Hello, World!\", FALSE)",
            Some(vec![
                vec![Literal::Int(7)],
                vec![Literal::Int(8)],
                vec![
                    Literal::String("Hello, World!".to_string()),
                    Literal::Bool(false),
                ],
            ]),
        );
    }

    #[test]
    fn test_insert_into() {
        fn check(input: &str, expected: Option<Input>) {
            parse_check(insert_into, input, expected)
        }

        check("nope", None);
        check("INSERT INTO", None);
        check("INSERT INTO foo", None);
        check("INSERT INTO foo VALUES", None);
        check("INSERT INTO foo (col1, col2) VALUES", None);
        check(
            "INSERT INTO foo (col1, col2) VALUES (\"val1\", 8)",
            Some(Input::InsertInto {
                table_name: "foo",
                columns: vec!["col1", "col2"],
                values: vec![vec![Literal::String("val1".to_string()), Literal::Int(8)]],
            }),
        );
        check(
            "INSERT INTO\nfoo\n(col1, col2)\nVALUES\n(\"val1\", 8)",
            Some(Input::InsertInto {
                table_name: "foo",
                columns: vec!["col1", "col2"],
                values: vec![vec![Literal::String("val1".to_string()), Literal::Int(8)]],
            }),
        );
        check(
            "INSERT INTO\nfoo\n(col1, col2)\nVALUES\n(\"val1\", 8),\n(\"val2\", TRUE)",
            Some(Input::InsertInto {
                table_name: "foo",
                columns: vec!["col1", "col2"],
                values: vec![
                    vec![Literal::String("val1".to_string()), Literal::Int(8)],
                    vec![Literal::String("val2".to_string()), Literal::Bool(true)],
                ],
            }),
        );
    }
}
