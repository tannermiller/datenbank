use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1, space1};
use nom::combinator::{map, opt, value};
use nom::error::{make_error, ErrorKind};
use nom::multi::many0;
use nom::sequence::{delimited, pair, tuple};
use nom::{Err as NomErr, IResult};

use super::literal::literal;
use super::{identifier, EqualityOp, Expression, Input, LogicalOp, SelectColumns, Terminal};

pub fn select_from(input: &str) -> IResult<&str, Input> {
    let (rest, (_, columns, _, table_name, where_clause)) = tuple((
        select_keyword,
        columns,
        from_keyword,
        table_name,
        where_clause,
    ))(input)?;

    Ok((
        rest,
        Input::SelectFrom {
            table_name,
            columns,
            where_clause,
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

fn where_clause(input: &str) -> IResult<&str, Option<Expression>> {
    opt(where_clause_required)(input)
}

fn where_clause_required(input: &str) -> IResult<&str, Expression> {
    let (rest, (_, expr)) = pair(where_keyword, expression)(input)?;
    Ok((rest, expr))
}

fn where_keyword(input: &str) -> IResult<&str, (&str, &str)> {
    pair(tag_no_case("where"), multispace1)(input)
}

fn terminal(input: &str) -> IResult<&str, Terminal> {
    alt((
        map(identifier, |id| Terminal::Identifier(id)),
        map(literal, |lit| Terminal::Literal(lit)),
    ))(input)
}

fn expression(input: &str) -> IResult<&str, Expression> {
    let (rest, (head_expr, tail_expr)) = pair(
        comparison,
        many0(tuple((multispace1, logical_op, multispace1, comparison))),
    )(input)?;

    enum Item<'a> {
        Comp(Expression<'a>),
        LogOp(LogicalOp),
    }

    let mut expr_stack = vec![Item::Comp(head_expr)];
    for (_, log_op, _, comp) in tail_expr {
        expr_stack.push(Item::LogOp(log_op));
        expr_stack.push(Item::Comp(comp));
    }

    enum Expecting<'a> {
        Nothing(Expression<'a>),
        CompExpr(LogicalOp, Expression<'a>),
    }

    let mut state = match expr_stack
        .pop()
        .expect("there is at least one item in the expr stack")
    {
        Item::Comp(comp) => Expecting::Nothing(comp),
        _ => unreachable!(),
    };

    for item in expr_stack.into_iter().rev() {
        state = match (state, item) {
            // handle all invalid states
            (Expecting::Nothing(_), Item::Comp(_)) | (Expecting::CompExpr(..), Item::LogOp(_)) => {
                return Err(NomErr::Failure(make_error(input, ErrorKind::Verify)))
            }

            // we've already got a complete expression, the only possible next state is to add
            // another logical operator (and subsequent expression)
            (Expecting::Nothing(expr), Item::LogOp(lo)) => Expecting::CompExpr(lo, expr),

            // we've got a logical operator, so now we complete the expression
            (Expecting::CompExpr(lo, expr), Item::Comp(comp)) => {
                Expecting::Nothing(Expression::Logical(Box::new(comp), lo, Box::new(expr)))
            }
        }
    }

    match state {
        Expecting::Nothing(expr) => Ok((rest, expr)),
        _ => Err(NomErr::Failure(make_error(input, ErrorKind::Verify))),
    }
}

fn logical_op(input: &str) -> IResult<&str, LogicalOp> {
    alt((
        value(LogicalOp::And, tag_no_case("and")),
        value(LogicalOp::Or, tag_no_case("or")),
    ))(input)
}

fn comparison(input: &str) -> IResult<&str, Expression> {
    let (rest, (term1, eq_op, term2)) = tuple((
        terminal,
        delimited(
            space1,
            alt((
                // We must put the two-char equality operators first in the alt so they're tried
                // first, otherwise we won't parse them correct as it will match the single-char
                // versions first.
                value(EqualityOp::NotEqual, tag("!=")),
                value(EqualityOp::GreaterThanOrEqualTo, tag(">=")),
                value(EqualityOp::LessThanOrEqualTo, tag("<=")),
                value(EqualityOp::GreaterThan, tag(">")),
                value(EqualityOp::LessThan, tag("<")),
                value(EqualityOp::Equal, tag("=")),
            )),
            space1,
        ),
        terminal,
    ))(input)?;
    Ok((rest, Expression::Comparison(term1, eq_op, term2)))
}

#[cfg(test)]
mod test {
    use super::super::{parse_check, Literal};
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
                where_clause: None,
            }),
        );
        check(
            "SELECT * FROM foo ",
            Some(Input::SelectFrom {
                table_name: "foo",
                columns: SelectColumns::Star,
                where_clause: None,
            }),
        );
        check(
            "SELECT bar, baz FROM foo",
            Some(Input::SelectFrom {
                table_name: "foo",
                columns: SelectColumns::Explicit(vec!["bar", "baz"]),
                where_clause: None,
            }),
        );
    }

    #[test]
    fn test_comparison() {
        fn check(input: &str, out: Option<Expression>) {
            parse_check(comparison, input, out)
        }

        check("nope", None);
        for eq_op in vec![
            EqualityOp::Equal,
            EqualityOp::NotEqual,
            EqualityOp::GreaterThan,
            EqualityOp::GreaterThanOrEqualTo,
            EqualityOp::LessThan,
            EqualityOp::LessThanOrEqualTo,
        ] {
            check(
                &format!("foo {} 7", eq_op),
                Some(Expression::Comparison(
                    Terminal::Identifier("foo"),
                    eq_op,
                    Terminal::Literal(Literal::Int(7)),
                )),
            );
            check(
                &format!("7 {} foo", eq_op),
                Some(Expression::Comparison(
                    Terminal::Literal(Literal::Int(7)),
                    eq_op,
                    Terminal::Identifier("foo"),
                )),
            );
            check(
                &format!("8 {} 7", eq_op),
                Some(Expression::Comparison(
                    Terminal::Literal(Literal::Int(8)),
                    eq_op,
                    Terminal::Literal(Literal::Int(7)),
                )),
            );
            check(
                &format!("foo             {}    7\n\n", eq_op),
                Some(Expression::Comparison(
                    Terminal::Identifier("foo"),
                    eq_op,
                    Terminal::Literal(Literal::Int(7)),
                )),
            );
        }
    }

    #[test]
    fn test_expression() {
        fn check(input: &str, out: Option<Expression>) {
            parse_check(expression, input, out)
        }

        check("nope", None);
        check(
            "foo = 7",
            Some(Expression::Comparison(
                Terminal::Identifier("foo"),
                EqualityOp::Equal,
                Terminal::Literal(Literal::Int(7)),
            )),
        );
        check(
            "foo = 7 AND bar > 2",
            Some(Expression::Logical(
                Box::new(Expression::Comparison(
                    Terminal::Identifier("foo"),
                    EqualityOp::Equal,
                    Terminal::Literal(Literal::Int(7)),
                )),
                LogicalOp::And,
                Box::new(Expression::Comparison(
                    Terminal::Identifier("bar"),
                    EqualityOp::GreaterThan,
                    Terminal::Literal(Literal::Int(2)),
                )),
            )),
        );
        check(
            "foo = 7 AND bar > 2 OR 3 <= baz",
            Some(Expression::Logical(
                Box::new(Expression::Comparison(
                    Terminal::Identifier("foo"),
                    EqualityOp::Equal,
                    Terminal::Literal(Literal::Int(7)),
                )),
                LogicalOp::And,
                Box::new(Expression::Logical(
                    Box::new(Expression::Comparison(
                        Terminal::Identifier("bar"),
                        EqualityOp::GreaterThan,
                        Terminal::Literal(Literal::Int(2)),
                    )),
                    LogicalOp::Or,
                    Box::new(Expression::Comparison(
                        Terminal::Literal(Literal::Int(3)),
                        EqualityOp::LessThanOrEqualTo,
                        Terminal::Identifier("baz"),
                    )),
                )),
            )),
        );
    }
}
