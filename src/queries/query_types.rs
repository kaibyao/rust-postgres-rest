use std::collections::HashMap;
use actix_web::{
    actix::{Message},
};
use failure::Error;

// get_all_table_columns types

#[derive(Serialize)]
/// Represents a single table column returned by get_all_table_columns
pub struct GetAllTableColumnsColumn {
    pub column_name: Option<String>,
    pub column_type: Option<String>,
    pub is_nullable: Option<bool>,
    pub default_value: Option<String>,
}

/// Convenience type alias
pub type GetAllTableColumnsResult = HashMap<String, Vec<GetAllTableColumnsColumn>>;

// used for sending queries

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    GetAllTableColumns,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    // QueryTable,
}

/// Represents a single database query to be sent via DbExecutor
pub struct Query {
    pub limit: i32,
    // need to add more (sort, WHERE filter, etc)
    pub task: QueryTasks,
    // pub sort_by: String
}

impl Message for Query {
    type Result = Result<QueryResult, Error>;
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    GetAllTableColumnsResult(GetAllTableColumnsResult)
}

