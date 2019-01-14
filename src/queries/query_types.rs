use actix_web::actix::Message;
use failure::Error;
// use r2d2_postgres::postgres::types::FromSql;
use std::collections::HashMap;

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

// query_table types

/// Represents a database table results, returned by a query
// pub type QueryResultRow = HashMap<String, Box<FromSql>>;
// pub type QueryResultRows = Vec<HashMap<String, _>>;
//
// used for sending queries

/// Represents a single database query to be sent via DbExecutor
pub struct Query {
    pub columns: Vec<String>,
    pub conditions: Option<String>,
    pub limit: Option<i32>,
    pub order_by: Option<String>,
    pub table: String,
    pub task: QueryTasks,
}

impl Message for Query {
    type Result = Result<QueryResult, Error>;
}

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    GetAllTableColumns,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    // QueryTable,
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    GetAllTableColumnsResult(GetAllTableColumnsResult),
    // QueryTable(Result<
    //     Vec<
    //         HashMap<String, FromSql>
    //     >,
    //     Error
    // >),
}
