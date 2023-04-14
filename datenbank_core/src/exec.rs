use crate::pagestore::{Error as PageStoreError, TablePageStoreBuilder};
use crate::parser::{
    self, ColumnSchema, ColumnType as ParserColumnType, EqualityOp, Expression, Input, Literal,
    LogicalOp, SelectColumns, Terminal as ParserTerm,
};
use crate::schema::{Column, ColumnType, Error as SchemaError, Schema};
use crate::table::{AllRows, Error as TableError, Table};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("table error")]
    Table(#[from] TableError),
    #[error("schema error")]
    Schema(#[from] SchemaError),
    #[error("page store error")]
    PageStore(#[from] PageStoreError),
    #[error("no such table {0}")]
    NoSuchTable(String),
    #[error("invalid where clause {0}")]
    InvalidWhereClause(String),
}

pub struct ExecResult {
    pub rows_affected: usize,
}

pub struct QueryResult {
    pub values: Vec<Vec<Column>>,
}

pub enum DatabaseResult {
    Exec(ExecResult),
    Query(QueryResult),
}

pub fn execute<B: TablePageStoreBuilder>(
    store_builder: &mut B,
    input: Input,
) -> Result<DatabaseResult, Error> {
    match input {
        Input::Create { table_name, schema } => create_table(store_builder, table_name, schema),
        Input::InsertInto {
            table_name,
            columns,
            values,
        } => insert_into(store_builder, table_name, columns, values),
        Input::SelectFrom {
            table_name,
            columns,
            where_clause,
        } => select_from(store_builder, table_name, columns, where_clause),
    }
}

fn create_table<B: TablePageStoreBuilder>(
    store_builder: &mut B,
    table_name: &str,
    schema: Vec<ColumnSchema>,
) -> Result<DatabaseResult, Error> {
    let schema = parser_schema_to_table_schema(schema)?;
    Table::create(table_name.to_string(), schema, store_builder)?;
    Ok(DatabaseResult::Exec(ExecResult { rows_affected: 0 }))
}

fn parser_schema_to_table_schema(parser_schema: Vec<ColumnSchema>) -> Result<Schema, Error> {
    let columns = parser_schema
        .into_iter()
        .map(|ps| {
            let ct = match ps.column_type {
                ParserColumnType::Int => ColumnType::Int,
                ParserColumnType::Bool => ColumnType::Bool,
                ParserColumnType::VarChar(size) => ColumnType::VarChar(size),
            };
            (ps.column_name.to_string(), ct)
        })
        .collect();

    Schema::new(columns).map_err(Into::into)
}

fn insert_into<B: TablePageStoreBuilder>(
    store_builder: &mut B,
    table_name: &str,
    columns: Vec<&str>,
    values: Vec<Vec<Literal>>,
) -> Result<DatabaseResult, Error> {
    let mut table = match Table::load(table_name, store_builder)? {
        Some(table) => table,
        None => return Err(Error::NoSuchTable(table_name.to_string())),
    };

    let column_values = table.schema().literals_to_columns(&columns, values)?;

    let rows_affected = table.insert(&columns, column_values)?;

    Ok(DatabaseResult::Exec(ExecResult { rows_affected }))
}

fn select_from<B: TablePageStoreBuilder>(
    store_builder: &mut B,
    table_name: &str,
    columns: SelectColumns,
    where_clause: Option<Expression>,
) -> Result<DatabaseResult, Error> {
    let mut table = match Table::load(table_name, store_builder)? {
        Some(table) => table,
        None => return Err(Error::NoSuchTable(table_name.to_string())),
    };

    let expanded_columns = table.schema().expand_select_columns(columns)?;

    let values = match where_clause {
        Some(wc) => {
            // TODO: We need to either:
            //   1) look for a complete set of key fields connected by AND, these don't need to be
            //      in key order, as long as there is no OR b/w them
            //   2) synthesize the Expression into a form that implements RowPredicate and pass
            //      that into Table::scan()

            let processed = process_expression(wc);
            todo!()
        }
        None => table.scan(expanded_columns, AllRows)?,
    };

    Ok(DatabaseResult::Query(QueryResult { values }))
}

#[derive(Debug, PartialEq)]
enum Terminal {
    Field(String),
    Literal(Literal),
}

impl From<parser::Terminal<'_>> for Terminal {
    fn from(t: parser::Terminal) -> Terminal {
        match t {
            parser::Terminal::Identifier(id) => Terminal::Field(id.to_string()),
            parser::Terminal::Literal(lit) => Terminal::Literal(lit),
        }
    }
}

#[derive(Debug, PartialEq)]
struct Comparison {
    left: Terminal,
    op: EqualityOp,
    right: Terminal,
    next: Option<Box<Logical>>,
}

#[derive(Debug, PartialEq)]
struct Logical {
    op: LogicalOp,
    right: Comparison,
}

fn process_expression(expr: Expression) -> Result<Comparison, Error> {
    match expr {
        Expression::Comparison(left, op, right) => Ok(Comparison {
            left: left.into(),
            op,
            right: right.into(),
            next: None,
        }),
        Expression::Logical(left, op, right) => process_logical(left, op, right),
    }
}

fn process_comparison(left: ParserTerm, op: EqualityOp, right: ParserTerm) -> Comparison {
    Comparison {
        left: left.into(),
        op,
        right: right.into(),
        next: None,
    }
}

fn process_logical(
    left: Box<Expression>,
    op: LogicalOp,
    right: Box<Expression>,
) -> Result<Comparison, Error> {
    let mut left = match *left {
        Expression::Comparison(l, o, r) => process_comparison(l, o, r),
        Expression::Logical(..) => {
            return Err(Error::InvalidWhereClause(
                "unexpected logical left of logical".to_string(),
            ))
        }
    };

    let right = match *right {
        Expression::Comparison(l, o, r) => process_comparison(l, o, r),
        Expression::Logical(l, o, r) => process_logical(l, o, r)?,
    };

    left.next = Some(Box::new(Logical { op, right }));

    Ok(left)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_process_expressions() {
        fn check(expr: Expression, expect: Option<Comparison>) {
            match (process_expression(expr), expect) {
                (Ok(result), Some(exp)) => assert_eq!(result, exp),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("test case shouldn't have passed"),
                (Err(err), Some(_)) => panic!("test case should't have errored: {err}"),
            }
        }

        check(
            Expression::Comparison(
                ParserTerm::Identifier("foo"),
                EqualityOp::Equal,
                ParserTerm::Literal(Literal::Int(7)),
            ),
            Some(Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: None,
            }),
        );
        check(
            Expression::Logical(
                Box::new(Expression::Comparison(
                    ParserTerm::Identifier("foo"),
                    EqualityOp::Equal,
                    ParserTerm::Literal(Literal::Int(7)),
                )),
                LogicalOp::And,
                Box::new(Expression::Comparison(
                    ParserTerm::Identifier("bar"),
                    EqualityOp::LessThan,
                    ParserTerm::Literal(Literal::String("joe".to_string())),
                )),
            ),
            Some(Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("bar".to_string()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("joe".to_string())),
                        next: None,
                    },
                })),
            }),
        )
    }
}
