use nom::bytes::complete::tag_no_case;
use nom::character::complete::{char, multispace0, multispace1, space0};
use nom::sequence::{delimited, pair};
use nom::IResult;

use super::{identifier, Input};

pub fn insert_into(input: &str) -> IResult<&str, Input> {
    todo!()
}

fn insert_into_literal(input: &str) -> IResult<&str, (&str, &str)> {
    pair(tag_no_case("insert into"), multispace1)(input)
}

fn insert_into_columns(input: &str) -> IResult<&str, Vec<&str>> {
    //delimited(char(')'), many1(), pair(char(')'), multispace1))
    todo!()
}

fn insert_into_column_field(input: &str) -> IResult<&str, &str> {
    delimited(space0, identifier, pair(char(','), multispace0))(input)
}

#[cfg(test)]
mod test {
    use super::super::parse_check;
    use super::*;

    #[test]
    fn test_insert_into_column_field() {
        fn check(input: &str, expected: Option<&str>) {
            parse_check(insert_into_column_field, input, expected)
        }

        check("nope", None);
        check("int", None);
        check("foo", Some("foo"));
    }
}
