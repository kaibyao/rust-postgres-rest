mod insert_into_table;
pub use self::insert_into_table::insert_into_table;

mod select_all_tables;
pub use select_all_tables::select_all_tables;

mod foreign_keys;

mod select_table_rows;
pub use self::select_table_rows::select_table_rows;

mod select_table_stats;
pub use self::select_table_stats::{
    select_column_stats, select_column_stats_statement, select_table_stats, TableColumnStat,
};

pub mod postgres_types;
pub mod query_types;

pub mod utils;

// pub fn upsert_into_table(conn: &Connection) {}

// pub fn delete_table_rows(conn: &Connection) {}

// fn update_table_rows(conn: &Connection) {}
