use std::rc::Rc;

use super::{DatabaseResult, Error, QueryResult};
use crate::cache::Cache;
use crate::key;
use crate::pagestore::{TablePageStore, TablePageStoreManager};
use crate::parser::{
    self, EqualityOp, Expression, Literal, LogicalOp, SelectColumns, Terminal as ParserTerm,
};
use crate::row::{AllRows, Error as RowError, Predicate, Row};
use crate::schema::{Column, ColumnType, Schema};
use crate::table::Table;

pub fn select_from<M: TablePageStoreManager>(
    store_manager: &mut M,
    table_name: &str,
    columns: SelectColumns,
    where_clause_expr: Option<Expression>,
) -> Result<DatabaseResult, Error> {
    let mut table = match Table::load(&mut store_manager.builder(table_name)?)? {
        Some(table) => table,
        None => return Err(Error::NoSuchTable(table_name.to_string())),
    };

    let expanded_columns = table.schema().expand_select_columns(columns)?;

    let values = match where_clause_expr {
        Some(expr) => where_clause(&mut table, expr, expanded_columns)?,
        None => Some(table.scan(&expanded_columns, AllRows)?),
    };

    Ok(DatabaseResult::Query(QueryResult {
        values: values.unwrap_or_else(|| Vec::new()),
    }))
}

fn where_clause<S: TablePageStore>(
    table: &mut Table<S>,
    expr: Expression,
    expanded_columns: Vec<Rc<String>>,
) -> Result<Option<Vec<Vec<Column>>>, Error> {
    // We will either:
    //   1) look for a complete set of key fields connected by AND, these don't need to be
    //      in key order, as long as there is no OR b/w them
    //   2) synthesize the Expression into a form that implements Predicate and pass
    //      that into Table::scan()

    let processed = process_expression(expr)?;

    let (primary_schema, secondary_schemas) = table.schemas();
    let strategy = determine_strategy(primary_schema, &secondary_schemas, &processed);

    match strategy {
        Strategy::FullScan => table.scan(&expanded_columns, processed).map(Some),

        Strategy::KeyLookup(ind, key) => match ind {
            Index::Primary => table.lookup(&key, &expanded_columns),
            Index::Secondary(name) => table.lookup_via_index(&name, &key, &expanded_columns),
        }
        .map(|r| r.map(|x| vec![x])),

        Strategy::RangeScan(_, _) => todo!(),
    }
    .map_err(Into::into)
}

#[derive(Debug, PartialEq)]
enum Index {
    Primary,
    Secondary(Rc<String>),
}

#[derive(Debug, PartialEq)]
enum Range {
    Closed(Vec<u8>, Vec<u8>),
    OpenLow(Vec<u8>),
    OpenHigh(Vec<u8>),
}

#[derive(Debug, PartialEq)]
enum Strategy {
    FullScan,
    KeyLookup(Index, Vec<u8>),
    RangeScan(Index, Range),
}

fn determine_strategy(
    primary: &Schema,
    secondaries: &[(Rc<String>, &Schema)],
    expr: &Comparison,
) -> Strategy {
    if let Some(key) = is_key_lookup(primary, expr) {
        return Strategy::KeyLookup(Index::Primary, key);
    }

    for (name, secondary) in secondaries {
        if let Some(key) = is_key_lookup(secondary, &expr) {
            return Strategy::KeyLookup(Index::Secondary(name.clone()), key);
        }
    }

    // TODO: Range Scans

    Strategy::FullScan
}

// Determine if this comparison is a direct key lookup (i.e. contains a complete key) for a key
// defined in the schema.
fn is_key_lookup(schema: &Schema, expr: &Comparison) -> Option<Vec<u8>> {
    // TODO:If we have a complete key and still more comparisons, do we do a lookup and apply a
    // predicate?

    let key_fields: Vec<&(Rc<String>, ColumnType)> = if let Some(pk) = schema.primary_key_columns()
    {
        pk.iter()
            .map(|k| {
                for col @ (col_name, _) in schema.columns() {
                    if col_name == k {
                        return col;
                    }
                }

                // we've already validated that the key definitely exists in the columns
                unreachable!()
            })
            .collect()
    } else {
        schema.columns().iter().collect()
    };

    // This should be the key len once we have keys that are less than the full schema.
    let mut key_parts = vec![None; key_fields.len()];

    let mut comp = expr;
    loop {
        if comp.op != EqualityOp::Equal {
            return None;
        }

        let (id, lit) = match (&comp.left, &comp.right) {
            (Terminal::Field(id), Terminal::Literal(lit))
            | (Terminal::Literal(lit), Terminal::Field(id)) => (id, lit),
            _ => return None,
        };

        let mut found = false;
        for (i, (col, ct)) in key_fields.iter().enumerate() {
            if id == col && ct.is_congruent_literal(lit) {
                key_parts[i] = Some(lit);
                found = true;
            }
        }

        // This is just until we start implementing key prefixes or something else that would allow it
        // to be ok to not find it.
        if !found {
            return None;
        }

        match &comp.next {
            Some(log) if log.op == LogicalOp::And => {
                comp = &log.right;
            }
            _ => break,
        }
    }

    let mut found_key_parts = Vec::with_capacity(key_fields.len());
    for kp in key_parts {
        match kp {
            Some(kp) => found_key_parts.push(kp),
            None => return None,
        };
    }

    Some(key::build(&found_key_parts))
}

#[derive(Clone, Debug, PartialEq)]
enum Terminal {
    Field(Rc<String>),
    Literal(Literal),
}

impl From<parser::Terminal<'_>> for Terminal {
    fn from(t: parser::Terminal) -> Terminal {
        match t {
            parser::Terminal::Identifier(id) => Terminal::Field(id.to_string().into()),
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

impl<S: TablePageStore> Predicate<S> for Comparison {
    fn is_satisfied_by(
        &self,
        schema: &Schema,
        data_cache: &mut Cache<S, Vec<u8>>,
        row: &Row,
    ) -> Result<bool, RowError> {
        use Terminal::*;

        let result = match (&self.left, self.op, &self.right) {
            (Field(l), op, Field(r)) => {
                let cols = row.to_columns(data_cache, schema, &[l.clone(), r.clone()])?;

                evaluate_equality_op(&cols[0], op, &cols[1])
            }

            (Field(l), op, Literal(r)) => {
                let cols = row.to_columns(data_cache, schema, &[l.clone()])?;
                let lits = schema.literals_to_columns(&[l], vec![vec![r.clone()]])?;

                evaluate_equality_op(&cols[0], op, &lits[0][0])
            }

            (Literal(l), op, Field(r)) => {
                let lits = schema.literals_to_columns(&[r], vec![vec![l.clone()]])?;
                let cols = row.to_columns(data_cache, schema, &[r.clone()])?;

                evaluate_equality_op(&cols[0], op, &lits[0][0])
            }

            (Literal(l), op, Literal(r)) => {
                use EqualityOp::*;
                match op {
                    Equal => l == r,
                    NotEqual => l != r,
                    GreaterThan => l > r,
                    GreaterThanOrEqualTo => l >= r,
                    LessThan => l < r,
                    LessThanOrEqualTo => l <= r,
                }
            }
        };

        // recurse down into the next logical op, if it exists
        match &self.next {
            Some(log) => {
                let next_result = log.right.is_satisfied_by(schema, data_cache, row)?;
                Ok(match log.op {
                    LogicalOp::And => result && next_result,
                    LogicalOp::Or => result || next_result,
                })
            }
            None => Ok(result),
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
    use crate::pagestore::{Memory, MemoryManager, TablePageStoreBuilder, TablePageStoreManager};
    use crate::row::{RowBytes, RowCol};

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
                left: Terminal::Field("foo".to_string().into()),
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
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("bar".to_string().into()),
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

        let schema = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(20)),
                ("baz".into(), ColumnType::Int),
            ],
            None,
            vec![],
        )
        .unwrap();
        let mut store_builder = MemoryManager::new(64 * 1024).builder("test").unwrap();
        let mut data_cache = Cache::new(store_builder.build().unwrap());
        let test_row = Row {
            body: vec![
                RowCol::Int(7),
                RowCol::Bool(false),
                RowCol::VarChar(RowBytes {
                    inline: "Hello, World!".into(),
                    next_page: None,
                }),
                RowCol::Int(3),
            ],
        };

        let valids = vec![
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: None,
            },
            Comparison {
                left: Terminal::Literal(Literal::Int(7)),
                op: EqualityOp::Equal,
                right: Terminal::Field("foo".to_string().into()),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
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
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::GreaterThan,
                right: Terminal::Field("baz".to_string().into()),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::Or,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::NotEqual,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(8)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::Or,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::NotEqual,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: Some(Box::new(Logical {
                            op: LogicalOp::And,
                            right: Comparison {
                                left: Terminal::Field("baz".to_string().into()),
                                op: EqualityOp::Equal,
                                right: Terminal::Literal(Literal::Int(3)),
                                next: None,
                            },
                        })),
                    },
                })),
            },
        ];

        let invalids = vec![
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(8)),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
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
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Field("baz".to_string().into()),
                next: None,
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::NotEqual,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::Equal,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: None,
                    },
                })),
            },
            Comparison {
                left: Terminal::Field("foo".to_string().into()),
                op: EqualityOp::Equal,
                right: Terminal::Literal(Literal::Int(7)),
                next: Some(Box::new(Logical {
                    op: LogicalOp::And,
                    right: Comparison {
                        left: Terminal::Field("qux".to_string().into()),
                        op: EqualityOp::LessThan,
                        right: Terminal::Literal(Literal::String("abc".to_string())),
                        next: Some(Box::new(Logical {
                            op: LogicalOp::And,
                            right: Comparison {
                                left: Terminal::Field("baz".to_string().into()),
                                op: EqualityOp::NotEqual,
                                right: Terminal::Literal(Literal::Int(3)),
                                next: None,
                            },
                        })),
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

    #[test]
    fn test_determine_strategy() {
        let one_field_schema =
            Schema::new(vec![("foo".into(), ColumnType::Int)], None, vec![]).unwrap();

        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &one_field_schema,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::LessThan,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: None,
                }
            )
        );
        assert_eq!(
            Strategy::KeyLookup(Index::Primary, vec![0, 0, 0, 7]),
            determine_strategy(
                &one_field_schema,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: None,
                }
            )
        );

        let multi_field_schema_no_primary_key = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(20)),
            ],
            None,
            vec![],
        )
        .unwrap();

        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &multi_field_schema_no_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::LessThan,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: None,
                }
            )
        );
        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &multi_field_schema_no_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: None,
                }
            )
        );
        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &multi_field_schema_no_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: Some(Box::new(Logical {
                        op: LogicalOp::And,
                        right: Comparison {
                            left: Terminal::Field("bar".to_string().into()),
                            op: EqualityOp::Equal,
                            right: Terminal::Literal(Literal::Bool(true)),
                            next: None,
                        }
                    })),
                }
            )
        );
        assert_eq!(
            Strategy::KeyLookup(
                Index::Primary,
                vec![0, 0, 0, 7, b'_', 1, b'_', b'k', b'a', b'b', b'o', b'o', b'm'],
            ),
            determine_strategy(
                &multi_field_schema_no_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: Some(Box::new(Logical {
                        op: LogicalOp::And,
                        right: Comparison {
                            left: Terminal::Field("bar".to_string().into()),
                            op: EqualityOp::Equal,
                            right: Terminal::Literal(Literal::Bool(true)),
                            next: Some(Box::new(Logical {
                                op: LogicalOp::And,
                                right: Comparison {
                                    left: Terminal::Field("qux".to_string().into()),
                                    op: EqualityOp::Equal,
                                    right: Terminal::Literal(Literal::String("kaboom".to_string())),
                                    next: None,
                                }
                            })),
                        }
                    })),
                }
            )
        );
        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &multi_field_schema_no_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: Some(Box::new(Logical {
                        op: LogicalOp::And,
                        right: Comparison {
                            left: Terminal::Field("bar".to_string().into()),
                            op: EqualityOp::Equal,
                            right: Terminal::Literal(Literal::Bool(true)),
                            next: Some(Box::new(Logical {
                                op: LogicalOp::And,
                                right: Comparison {
                                    left: Terminal::Field("qux".to_string().into()),
                                    op: EqualityOp::GreaterThan,
                                    right: Terminal::Literal(Literal::String("kaboom".to_string())),
                                    next: None,
                                }
                            })),
                        }
                    })),
                }
            )
        );

        let multi_field_schema_with_primary_key = Schema::new(
            vec![
                ("foo".into(), ColumnType::Int),
                ("bar".into(), ColumnType::Bool),
                ("qux".into(), ColumnType::VarChar(20)),
            ],
            Some(vec!["foo".into(), "bar".into()]),
            vec![],
        )
        .unwrap();

        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &multi_field_schema_with_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::LessThan,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: None,
                }
            )
        );
        assert_eq!(
            Strategy::FullScan,
            determine_strategy(
                &multi_field_schema_with_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: None,
                }
            )
        );
        assert_eq!(
            Strategy::KeyLookup(Index::Primary, vec![0, 0, 0, 7, b'_', 1,]),
            determine_strategy(
                &multi_field_schema_with_primary_key,
                &[],
                &Comparison {
                    left: Terminal::Field("foo".to_string().into()),
                    op: EqualityOp::Equal,
                    right: Terminal::Literal(Literal::Int(7)),
                    next: Some(Box::new(Logical {
                        op: LogicalOp::And,
                        right: Comparison {
                            left: Terminal::Field("bar".to_string().into()),
                            op: EqualityOp::Equal,
                            right: Terminal::Literal(Literal::Bool(true)),
                            next: None,
                        }
                    })),
                }
            )
        );
    }
}
