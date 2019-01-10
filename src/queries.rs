use actix_web::{
    actix::{Message},
};
use failure::Error;
use crate::db::Connection;

pub enum Tasks {
    GetAllTableFields,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    // QueryTable,
}

pub struct Queries {
    pub limit: i32,
    // need to add more (sort, WHERE filter, etc)
    pub task: Tasks,
}

impl Message for Queries {
    type Result = Result<Vec<String>, Error>;
}

pub fn get_all_tables(conn: &Connection) -> Result<Vec<String>, Error> {
    let statement = "
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_type='BASE TABLE';";
    let prep_statement = conn.prepare(statement)?;

    let rows: Vec<String> = prep_statement.query(&[])?.iter().map(|row| row.get(0)).collect();
    Ok(rows)
}

// pub fn query_table(conn: &Connection) {}

// pub fn insert_into_table(conn: &Connection) {}

// pub fn upsert_into_table(conn: &Connection) {}

// pub fn delete_table_rows(conn: &Connection) {}

// fn update_table_rows(conn: &Connection) {}
