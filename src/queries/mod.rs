mod get_all_table_columns;
pub use self::get_all_table_columns::get_all_table_columns;

mod query_table;
pub use self::query_table::query_table;

pub mod query_types;

mod utils;

// pub fn query_table(conn: &Connection) {}

// pub fn insert_into_table(conn: &Connection) {}

// pub fn upsert_into_table(conn: &Connection) {}

// pub fn delete_table_rows(conn: &Connection) {}

// fn update_table_rows(conn: &Connection) {}
