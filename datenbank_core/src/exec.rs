use crate::cache::Cache;
use crate::pagestore::{Error as PageStoreError, TablePageStore, TablePageStoreBuilder};
use crate::parser::{
    self, ColumnSchema, ColumnType as ParserColumnType, EqualityOp, Expression, Input, Literal,
    LogicalOp, SelectColumns, Terminal as ParserTerm,
};
use crate::row::{AllRows, Error as RowError, Predicate, Row};
use crate::schema::{Column, ColumnType, Error as SchemaError, Schema};
use crate::table::{Error as TableError, Table};

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
            // We will either:
            //   1) look for a complete set of key fields connected by AND, these don't need to be
            //      in key order, as long as there is no OR b/w them
            //   2) synthesize the Expression into a form that implements Predicate and pass
            //      that into Table::scan()

            let processed = process_expression(wc)?;
            match is_key_lookup(table.schema(), &processed) {
                Some(key) => match table.lookup(&key)? {
                    Some(res) => vec![res],
                    None => vec![],
                },
                None => table.scan(expanded_columns, processed)?,
            }
        }
        None => table.scan(expanded_columns, AllRows)?,
    };

    Ok(DatabaseResult::Query(QueryResult { values }))
}

#[derive(Clone, Debug, PartialEq)]
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
        Expression::Logical(left, op, right) => process_logical(*left, op, *right),
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
    left: Expression,
    op: LogicalOp,
    right: Expression,
) -> Result<Comparison, Error> {
    let mut left = match left {
        Expression::Comparison(l, o, r) => process_comparison(l, o, r),
        Expression::Logical(..) => {
            return Err(Error::InvalidWhereClause(
                "unexpected logical left of logical".to_string(),
            ))
        }
    };

    let right = match right {
        Expression::Comparison(l, o, r) => process_comparison(l, o, r),
        Expression::Logical(l, o, r) => process_logical(*l, o, *r)?,
    };

    left.next = Some(Box::new(Logical { op, right }));

    Ok(left)
}

fn is_key_lookup(schema: &Schema, expr: &Comparison) -> Option<Vec<u8>> {
    todo!()
}

impl<S: TablePageStore> Predicate<S> for Comparison {
    fn is_satisfied_by(
        &self,
        schema: &Schema,
        data_cache: &mut Cache<S, Vec<u8>>,
        row: &Row,
    ) -> Result<bool, RowError> {
        use Terminal::*;

        match (&self.left, self.op, &self.right) {
            (Field(l), op, Field(r)) => {
                let cols = row.to_columns(data_cache, schema, &[l.to_string(), r.to_string()])?;

                Ok(evaluate_equality_op(&cols[0], op, &cols[1]))
            }

            (Field(l), op, Literal(r)) => {
                let cols = row.to_columns(data_cache, schema, &[l.to_string()])?;
                let lits = schema.literals_to_columns(&[l], vec![vec![r.clone()]])?;

                Ok(evaluate_equality_op(&cols[0], op, &lits[0][0]))
            }

            (Literal(l), op, Field(r)) => {
                let lits = schema.literals_to_columns(&[r], vec![vec![l.clone()]])?;
                let cols = row.to_columns(data_cache, schema, &[r.to_string()])?;

                Ok(evaluate_equality_op(&cols[0], op, &lits[0][0]))
            }

            (Literal(l), op, Literal(r)) => {
                use EqualityOp::*;
                Ok(match op {
                    Equal => l == r,
                    NotEqual => l != r,
                    GreaterThan => l > r,
                    GreaterThanOrEqualTo => l >= r,
                    LessThan => l < r,
                    LessThanOrEqualTo => l <= r,
                })
            }
        }
    }
}

fn evaluate_equality_op(left: &Column, op: EqualityOp, right: &Column) -> bool {
    use EqualityOp::*;
    match op {
        Equal => left == right,
        NotEqual => left != right,
        GreaterThan => left > right,
        GreaterThanOrEqualTo => left >= right,
        LessThan => left < right,
        LessThanOrEqualTo => left <= right,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pagestore::{Memory, MemoryBuilder};
    use crate::row::{RowCol, RowVarChar};

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

    #[test]
    fn test_comparison_predicate() {
        fn check(
            schema: &Schema,
            dc: &mut Cache<Memory, Vec<u8>>,
            comp: Comparison,
            row: &Row,
            expect: Option<bool>,
        ) {
            match (comp.is_satisfied_by(schema, dc, row), expect) {
                (Ok(result), Some(exp)) => assert_eq!(result, exp),
                (Err(_), None) => (),
                (Ok(_), None) => panic!("test case shouldn't have passed"),
                (Err(err), Some(_)) => panic!("test case should't have errored: {err}"),
            }
        }

        let schema = Schema::new(vec![
            ("foo".into(), ColumnType::Int),
            ("bar".into(), ColumnType::Bool),
            ("qux".into(), ColumnType::VarChar(20)),
            ("baz".into(), ColumnType::Int),
        ])
        .unwrap();
        let mut store_builder = MemoryBuilder::new(64 * 1024);
        let mut data_cache = Cache::new(store_builder.build("test").unwrap());
        let test_row = Row {
            body: vec![
                RowCol::Int(7),
                RowCol::Bool(false),
                RowCol::VarChar(RowVarChar {
                    inline: "Hello, World!".into(),
                    next_page: None,
                }),
                RowCol::Int(3),
            ],
        };

        let valids = vec![
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: None,
            },
            Comparison {
                left: Terminal::Literal(Literal::Int(7)),
                op: EqualityOp::Equal,
                right: Terminal::Field("foo".to_string()),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::LessThan,
                right: Terminal::Literal(Literal::Int(10)),
                next: None,
            },
            Comparison {
                left: Terminal::Literal(Literal::Int(10)),
                op: EqualityOp::LessThanOrEqualTo,
                right: Terminal::Literal(Literal::Int(10)),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::GreaterThan,
                right: Terminal::Field("baz".to_string()),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
        ];

        let invalids = vec![
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(8)),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::GreaterThan,
                right: Terminal::Literal(Literal::Int(10)),
                next: None,
            },
            Comparison {
                left: Terminal::Literal(Literal::Int(10)),
                op: EqualityOp::LessThan,
                right: Terminal::Literal(Literal::Int(10)),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::Equal,
                right: Terminal::Field("baz".to_string()),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string()),
                op: EqualityOp::NotEqual,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
        ];

        for valid in valids {
            check(&schema, &mut data_cache, valid, &test_row, Some(true));
        }

        for invalid in invalids {
            check(&schema, &mut data_cache, invalid, &test_row, Some(false));
        }
    }
}
