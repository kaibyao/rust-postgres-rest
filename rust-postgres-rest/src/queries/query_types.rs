use super::postgres_types::RowValues;
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Debug)]
/// Represents a single DELETE query
pub struct DeleteParams {
    pub table: String,
    pub conditions: Option<String>,
    pub confirm_delete: Option<String>,
    pub returning_columns: Option<Vec<String>>,
}

#[derive(Debug)]
/// Represents a custom SQL query
pub struct ExecuteParams {
    pub statement: String,
    pub is_return_rows: bool,
}

#[derive(Debug)]
/// Represents a single SELECT query
pub struct SelectParams {
    pub distinct: Option<Vec<String>>,
    pub columns: Vec<String>,
    pub table: String,
    pub conditions: Option<String>,
    pub group_by: Option<Vec<String>>,
    pub order_by: Option<Vec<String>>,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug)]
/// Represents a single INSERT query
pub struct InsertParams {
    pub conflict_action: Option<String>,
    pub conflict_target: Option<Vec<String>>,
    pub returning_columns: Option<Vec<String>>,
    pub rows: Vec<Map<String, Value>>,
    pub table: String,
}

#[derive(Debug)]
/// Parameters used to generate an `UPDATE` SQL statement.
pub struct UpdateParams {
    /// A JSON object whose key-values represent column names and the values to set.
    pub column_values: Map<String, Value>,
    /// WHERE expression.
    pub conditions: Option<String>,
    /// List of (foreign key) columns whose values are returned.
    pub returning_columns: Option<Vec<String>>,
    // Name of table to update.
    pub table: String,
}

#[derive(Serialize)]
pub struct RowsAffectedQueryResult {
    num_rows: u64,
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    QueryTableResult(Vec<RowValues>),
    RowsAffected(RowsAffectedQueryResult),
}

impl QueryResult {
    pub fn from_num_rows_affected(num_rows: u64) -> Self {
        QueryResult::RowsAffected(RowsAffectedQueryResult { num_rows })
    }
}
