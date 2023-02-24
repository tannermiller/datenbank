use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alphanumeric1, char, i32 as char_i32, one_of};
use nom::combinator::value;
use nom::multi::many0;
use nom::sequence::delimited;
use nom::IResult;

use super::Literal;

pub fn literal(input: &str) -> IResult<&str, Literal> {
    alt((parse_bool, parse_int, parse_string))(input)
}

fn parse_bool(input: &str) -> IResult<&str, Literal> {
    let (rest, bool_val) = alt((
        value(true, tag_no_case("true")),
        value(false, tag_no_case("false")),
    ))(input)?;
    Ok((rest, Literal::Bool(bool_val)))
}

fn parse_int(input: &str) -> IResult<&str, Literal> {
    let (rest, int_val) = char_i32(input)?;
    Ok((rest, Literal::Int(int_val)))
}

// strings are enclosed in quotations marks and are ascii and the following escaped characters:
//   - \t, \n, \", \\
fn parse_string(input: &str) -> IResult<&str, Literal> {
    delimited(char('"'), parse_string_no_quotes, char('"'))(input)
}

#[derive(Debug, PartialEq)]
enum StringPart<'a> {
    Str(&'a str),
    Char(char),
}

fn parse_string_no_quotes(input: &str) -> IResult<&str, Literal> {
    let (rest, string_parts) = many0(alt((
        parse_alphanum,
        parse_punctuation,
        parse_escaped_character,
    )))(input)?;
    let str_val = string_parts.into_iter().fold(String::new(), |mut acc, sp| {
        match sp {
            StringPart::Str(s) => acc.push_str(s),
            StringPart::Char(c) => acc.push(c),
        };
        acc
    });
    Ok((rest, Literal::String(str_val)))
}

fn parse_alphanum(input: &str) -> IResult<&str, StringPart> {
    let (rest, strs) = alphanumeric1(input)?;
    Ok((rest, StringPart::Str(strs)))
}

fn parse_punctuation(input: &str) -> IResult<&str, StringPart> {
    let (rest, ch) = one_of(" !@#$%^&*()-_+=,.<>?/:;[]{}|~`")(input)?;
    Ok((rest, StringPart::Char(ch)))
}

fn parse_escaped_character(input: &str) -> IResult<&str, StringPart> {
    let (rest, ch) = alt((
        value('\t', tag(r"\t")),
        value('\n', tag(r"\n")),
        value('\"', tag("\\\"")),
        value('\\', tag("\\\\")),
    ))(input)?;
    Ok((rest, StringPart::Char(ch)))
}

#[cfg(test)]
mod test {
    use super::super::parse_check;
    use super::*;

    #[test]
    fn test_literal() {
        fn check(input: &str, expected: Option<Literal>) {
            parse_check(literal, input, expected)
        }

        check("7", Some(Literal::Int(7)));
        check("TRUE", Some(Literal::Bool(true)));
        check("FALSE", Some(Literal::Bool(false)));
        check(
            "\"Hello, world!\"",
            Some(Literal::String("Hello, world!".to_string())),
        );
    }

    #[test]
    fn test_parse_bool() {
        fn check(input: &str, expected: Option<Literal>) {
            parse_check(parse_bool, input, expected)
        }

        check("Nope", None);
        check("true", Some(Literal::Bool(true)));
        check("TRUE", Some(Literal::Bool(true)));
        check("True", Some(Literal::Bool(true)));
        check("tRuE", Some(Literal::Bool(true)));
        check("false", Some(Literal::Bool(false)));
        check("FALSE", Some(Literal::Bool(false)));
        check("False", Some(Literal::Bool(false)));
        check("fAlSe", Some(Literal::Bool(false)));
    }

    #[test]
    fn test_parse_int() {
        fn check(input: &str, expected: Option<Literal>) {
            parse_check(parse_int, input, expected)
        }

        check("none", None);
        check("0", Some(Literal::Int(0)));
        check("7", Some(Literal::Int(7)));
        check("33838383", Some(Literal::Int(33838383)));
        check("-4828", Some(Literal::Int(-4828)));
    }

    #[test]
    fn test_parse_escaped_character() {
        fn check(input: &str, expected: Option<StringPart>) {
            parse_check(parse_escaped_character, input, expected)
        }

        check("nope", None);
        check("\\t", Some(StringPart::Char('\t')));
        check("\\n", Some(StringPart::Char('\n')));
        check("\\\"", Some(StringPart::Char('"')));
        check("\\\\", Some(StringPart::Char('\\')));
    }

    #[test]
    fn test_parse_alphanum() {
        fn check(input: &str, expected: Option<StringPart>) {
            parse_check(parse_alphanum, input, expected)
        }

        check("yep", Some(StringPart::Str("yep")));
        check("1y1ep1", Some(StringPart::Str("1y1ep1")));
        check("\\t", None);
    }

    #[test]
    fn test_punctuation() {
        fn check(input: &str, expected: Option<StringPart>) {
            parse_check(parse_punctuation, input, expected)
        }

        check("nope", None);
        check("$", Some(StringPart::Char('$')));
        check("^", Some(StringPart::Char('^')));
        check("_", Some(StringPart::Char('_')));
        check(" ", Some(StringPart::Char(' ')));
    }

    #[test]
    fn test_parse_string_no_quotes() {
        fn check(input: &str, expected: Option<Literal>) {
            parse_check(parse_string_no_quotes, input, expected)
        }

        check("yep", Some(Literal::String("yep".to_string())));
        check(
            "Hello, World!",
            Some(Literal::String("Hello, World!".to_string())),
        );
        check(
            " Hello,\\n\\tWorld!_$%123",
            Some(Literal::String(" Hello,\n\tWorld!_$%123".to_string())),
        );
    }

    #[test]
    fn test_parse_string() {
        fn check(input: &str, expected: Option<Literal>) {
            parse_check(parse_string, input, expected)
        }

        check("\"yep\"", Some(Literal::String("yep".to_string())));
        check(
            "\"Hello, World!\"",
            Some(Literal::String("Hello, World!".to_string())),
        );
        check(
            "\" Hello,\\n\\tWorld!_$%123\"",
            Some(Literal::String(" Hello,\n\tWorld!_$%123".to_string())),
        );
    }
}