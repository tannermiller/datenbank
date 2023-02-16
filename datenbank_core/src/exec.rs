use crate::pagestore::{Error as PageStoreError, TablePageStoreBuilder};
use crate::parser::{ColumnSchema, ColumnType as ParserColumnType, Input};
use crate::schema::{ColumnType, Error as SchemaError, Schema};
use crate::table::{Error as TableError, Table};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {
    #[error("table error")]
    Table(#[from] TableError),
    #[error("schema error")]
    Schema(#[from] SchemaError),
    #[error("page store error")]
    PageStore(#[from] PageStoreError),
}

pub struct ExecResult {
    pub rows_affected: usize,
}

pub fn execute<B: TablePageStoreBuilder>(
    store_builder: &mut B,
    input: Input,
) -> Result<ExecResult, Error> {
    match input {
        Input::Create { table_name, schema } => create_table(store_builder, table_name, schema),
    }
}

fn create_table<B: TablePageStoreBuilder>(
    store_builder: &mut B,
    table_name: &str,
    schema: Vec<ColumnSchema>,
) -> Result<ExecResult, Error> {
    let store = store_builder.build(table_name)?;
    let schema = parser_schema_to_table_schema(schema)?;
    Table::create(table_name.to_string(), schema, store)?;
    Ok(ExecResult { rows_affected: 0 })
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
