mod get_all_tables;
pub use self::get_all_tables::get_all_tables;

mod query_table;
pub use self::query_table::query_table;

mod query_table_stats;
pub use self::query_table_stats::get_table_stats;

pub mod query_types;

pub mod utils;

// pub fn insert_into_table(conn: &Connection) {}

// pub fn upsert_into_table(conn: &Connection) {}

// pub fn delete_table_rows(conn: &Connection) {}

// fn update_table_rows(conn: &Connection) {}
