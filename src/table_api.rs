use crate::actix_web::Result;
use crate::postgres::Connection;

#[derive(Serialize)]
pub struct ApiError {
    pub message: String,
}

pub fn prepare_all_statements(conn: &Connection) {
    prepare_query_table(conn);
    prepare_insert_into_table(conn);
    prepare_upsert_into_table(conn);
    prepare_delete_table_rows(conn);
    prepare_update_table_rows(conn);
}

pub fn get_all_tables(conn: &Connection) -> Result<Vec<String>, postgres::Error> {
    let tables = conn
        .query(
            "
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public'
    AND table_type='BASE TABLE';",
            &[],
        )?
        .iter()
        .map(|row| row.get(0))
        .collect();
    Ok(tables)
}

fn prepare_query_table(conn: &Connection) {}
pub fn query_table(conn: &Connection) {}

fn prepare_insert_into_table(conn: &Connection) {}
pub fn insert_into_table(conn: &Connection) {}

fn prepare_upsert_into_table(conn: &Connection) {}
pub fn upsert_into_table(conn: &Connection) {}

fn prepare_delete_table_rows(conn: &Connection) {}
pub fn delete_table_rows(conn: &Connection) {}

fn prepare_update_table_rows(conn: &Connection) {}
fn update_table_rows(conn: &Connection) {}
