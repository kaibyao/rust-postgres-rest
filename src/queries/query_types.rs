use std::collections::HashMap;
use actix_web::{
    actix::{Message},
};
use failure::Error;

// get_all_table_columns types

pub struct GetAllTableColumnsColumn {
    column_name: String,
    column_type: String,
    is_nullable: bool,
    default_value: String,
}

pub type GetAllTableColumnsResult = HashMap<String, Vec<GetAllTableColumnsColumn>>;

// used for sending queries

pub enum Tasks {
    GetAllTableColumns,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    // QueryTable,
}

pub struct Query {
    pub limit: i32,
    // need to add more (sort, WHERE filter, etc)
    pub task: Tasks,
    // pub sort_by: String
}

impl Message for Query {
    type Result = Result<QueryResult, Error>;
}

pub enum QueryResult {
    GetAllTableColumnsResult
}

