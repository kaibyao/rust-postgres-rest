mod create_table;
pub use self::create_table::create_table;

mod get_all_tables;
pub use self::get_all_tables::get_all_tables;

mod query_table;
pub use self::query_table::query_table;

mod table_stats;
pub use self::table_stats::get_table_stats;

pub mod postgres_types;
pub mod query_types;

pub mod utils;

// pub fn insert_into_table(conn: &Connection) {}

// pub fn upsert_into_table(conn: &Connection) {}

// pub fn delete_table_rows(conn: &Connection) {}

// fn update_table_rows(conn: &Connection) {}
