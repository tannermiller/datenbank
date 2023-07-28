use crate::pagestore::{Error as PageStoreError, TablePageStoreManager};
use crate::parser::{ColumnSchema, ColumnType as ParserColumnType, Index, Input, Literal};
use crate::schema::{Column, ColumnType, Error as SchemaError, Schema};
use crate::table::{Error as TableError, Table};

mod query;

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

#[derive(Debug, PartialEq)]
pub struct ExecResult {
    pub rows_affected: usize,
}

#[derive(Debug, PartialEq)]
pub struct QueryResult {
    pub values: Vec<Vec<Column>>,
}

#[derive(Debug, PartialEq)]
pub enum DatabaseResult {
    Exec(ExecResult),
    Query(QueryResult),
}

pub fn execute<M: TablePageStoreManager>(
    store_manager: &mut M,
    input: Input,
) -> Result<DatabaseResult, Error> {
    match input {
        Input::Create {
            table_name,
            columns,
            primary_key,
            indices,
        } => create_table(store_manager, table_name, columns, primary_key, indices),
        Input::InsertInto {
            table_name,
            columns,
            values,
        } => insert_into(store_manager, table_name, columns, values),
        Input::SelectFrom {
            table_name,
            columns,
            where_clause,
        } => query::select_from(store_manager, table_name, columns, where_clause),
    }
}

fn create_table<M: TablePageStoreManager>(
    store_manager: &mut M,
    table_name: &str,
    columns: Vec<ColumnSchema>,
    primary_key: Option<Vec<&str>>,
    indices: Vec<Index>,
) -> Result<DatabaseResult, Error> {
    let schema = parser_schema_to_table_schema(columns, primary_key, indices)?;
    Table::create(
        table_name.to_string(),
        schema,
        &mut store_manager.builder(table_name)?,
    )?;
    Ok(DatabaseResult::Exec(ExecResult { rows_affected: 0 }))
}

fn parser_schema_to_table_schema(
    parser_schema: Vec<ColumnSchema>,
    primary_key: Option<Vec<&str>>,
    indices: Vec<Index>,
) -> Result<Schema, Error> {
    let columns = parser_schema
        .into_iter()
        .map(|ps| {
            let ct = match ps.column_type {
                ParserColumnType::Int => ColumnType::Int,
                ParserColumnType::Bool => ColumnType::Bool,
                ParserColumnType::VarChar(size) => ColumnType::VarChar(size),
                ParserColumnType::LongBlob(size) => ColumnType::LongBlob(size),
            };
            (ps.column_name.to_string(), ct)
        })
        .collect();

    let indices = indices
        .into_iter()
        .map(|idx| (idx.name, idx.columns))
        .collect();

    Schema::new(columns, primary_key, indices).map_err(Into::into)
}

fn insert_into<M: TablePageStoreManager>(
    store_manager: &mut M,
    table_name: &str,
    columns: Vec<&str>,
    values: Vec<Vec<Literal>>,
) -> Result<DatabaseResult, Error> {
    let mut table = match Table::load(&mut store_manager.builder(table_name)?)? {
        Some(table) => table,
        None => return Err(Error::NoSuchTable(table_name.to_string())),
    };

    let column_values = table.schema().literals_to_columns(&columns, values)?;

    let rows_affected = table.insert(&columns, column_values)?;

    Ok(DatabaseResult::Exec(ExecResult { rows_affected }))
}
