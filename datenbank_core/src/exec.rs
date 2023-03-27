use crate::pagestore::{Error as PageStoreError, TablePageStoreBuilder};
use crate::parser::{ColumnSchema, ColumnType as ParserColumnType, Input, Literal, SelectColumns};
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
        } => select_from(store_builder, table_name, columns),
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
) -> Result<DatabaseResult, Error> {
    let mut table = match Table::load(table_name, store_builder)? {
        Some(table) => table,
        None => return Err(Error::NoSuchTable(table_name.to_string())),
    };

    let expanded_columns = table.schema().expand_select_columns(columns)?;

    let values = table.scan(expanded_columns)?;

    Ok(DatabaseResult::Query(QueryResult { values }))
}
