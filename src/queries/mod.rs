use actix_web::{
    actix::{Message},
};
use failure::Error;

mod get_all_table_columns;
pub use self::get_all_table_columns::{GetAllTableColumnsResult, get_all_table_columns};

pub enum Tasks {
    GetAllTableColumns,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    // QueryTable,
}

pub enum QueryResult {
    GetAllTableColumnsResult
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

// pub fn query_table(conn: &Connection) {}

// pub fn insert_into_table(conn: &Connection) {}

// pub fn upsert_into_table(conn: &Connection) {}

// pub fn delete_table_rows(conn: &Connection) {}

// fn update_table_rows(conn: &Connection) {}
