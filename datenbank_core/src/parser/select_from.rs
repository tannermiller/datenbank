use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::multi::many0;
use nom::sequence::{pair, tuple};
use nom::IResult;

use super::{identifier, Input, SelectColumns};

pub fn select_from(input: &str) -> IResult<&str, Input> {
    let (rest, (_, columns, _, table_name)) =
        tuple((select_keyword, columns, from_keyword, table_name))(input)?;

    Ok((
        rest,
        Input::SelectFrom {
            table_name,
            columns,
        },
    ))
}

fn select_keyword(input: &str) -> IResult<&str, (&str, &str)> {
    pair(tag_no_case("select"), multispace1)(input)
}

fn from_keyword(input: &str) -> IResult<&str, (&str, &str)> {
    pair(tag_no_case("from"), multispace1)(input)
}

fn table_name(input: &str) -> IResult<&str, &str> {
    let (rest, (table_name, _)) = pair(identifier, multispace0)(input)?;
    Ok((rest, table_name))
}

fn columns(input: &str) -> IResult<&str, SelectColumns> {
    alt((star, multi_columns))(input)
}

fn star(input: &str) -> IResult<&str, SelectColumns> {
    let (rest, _) = pair(tag("*"), multispace1)(input)?;
    Ok((rest, SelectColumns::Star))
}

fn multi_columns(input: &str) -> IResult<&str, SelectColumns> {
    let (rest, (first, others, _)) = tuple((
        identifier,
        many0(tuple((tag(","), multispace0, identifier))),
        multispace1,
    ))(input)?;
    let mut cols: Vec<&str> = others.into_iter().map(|(_, _, col)| col).collect();
    cols.insert(0, first);
    Ok((rest, SelectColumns::Explicit(cols)))
}

#[cfg(test)]
mod test {
    use super::super::parse_check;
    use super::*;

    #[test]
    fn test_select_columns() {
        fn check(input: &str, out: Option<SelectColumns>) {
            parse_check(columns, input, out)
        }

        check("nope", None);
        check("* ", Some(SelectColumns::Star));
        check("*    \n   \t ", Some(SelectColumns::Star));
        check("foo1 ", Some(SelectColumns::Explicit(vec!["foo1"])));
        check(
            "foo1, Foo2 ",
            Some(SelectColumns::Explicit(vec!["foo1", "Foo2"])),
        );
        check(
            "foo1,\nFoo2    ",
            Some(SelectColumns::Explicit(vec!["foo1", "Foo2"])),
        );
        check(
            "no,spaces,but,here ",
            Some(SelectColumns::Explicit(vec!["no", "spaces", "but", "here"])),
        )
    }

    #[test]
    fn test_select_from() {
        fn check(input: &str, out: Option<Input>) {
            parse_check(select_from, input, out)
        }

        check("nope", None);
        check(
            "select * from foo\n",
            Some(Input::SelectFrom {
                table_name: "foo",
                columns: SelectColumns::Star,
            }),
        );
        check(
            "SELECT * FROM foo ",
            Some(Input::SelectFrom {
                table_name: "foo",
                columns: SelectColumns::Star,
            }),
        );
        check(
            "SELECT bar, baz FROM foo",
            Some(Input::SelectFrom {
                table_name: "foo",
                columns: SelectColumns::Explicit(vec!["bar", "baz"]),
            }),
        );
    }
}
