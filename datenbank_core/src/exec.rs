use crate::pagestore::{Error as PageStoreError, TablePageStoreBuilder};
use crate::parser::{ColumnSchema, ColumnType as ParserColumnType, Input, Literal};
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
    #[error("no such table {0}")]
    NoSuchTable(String),
}

pub struct ExecResult {
    pub rows_affected: usize,
}

pub fn execute<B: TablePageStoreBuilder>(
    store_builder: B,
    input: Input,
) -> Result<ExecResult, Error> {
    match input {
        Input::Create { table_name, schema } => create_table(store_builder, table_name, schema),
        Input::InsertInto {
            table_name,
            columns,
            values,
        } => insert_into(store_builder, table_name, columns, values),
    }
}

fn create_table<B: TablePageStoreBuilder>(
    store_builder: B,
    table_name: &str,
    schema: Vec<ColumnSchema>,
) -> Result<ExecResult, Error> {
    let schema = parser_schema_to_table_schema(schema)?;
    Table::create(table_name.to_string(), schema, store_builder)?;
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

fn insert_into<B: TablePageStoreBuilder>(
    store_builder: B,
    table_name: &str,
    columns: Vec<&str>,
    values: Vec<Vec<Literal>>,
) -> Result<ExecResult, Error> {
    let table = match Table::load(table_name, store_builder)? {
        Some(table) => table,
        None => return Err(Error::NoSuchTable(table_name.to_string())),
    };

    todo!()
}
