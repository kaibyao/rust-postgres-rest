mod foreign_keys;

mod delete_table_rows;
pub use self::delete_table_rows::{delete_table_rows, DeleteParams};

mod execute_sql_query;
pub use self::execute_sql_query::{execute_sql_query, ExecuteParams};

mod insert_into_table;
pub use self::insert_into_table::{insert_into_table, InsertParams};

mod select_all_tables;
pub use select_all_tables::select_all_tables;

mod select_table_rows;
pub use self::select_table_rows::{select_table_rows, SelectParams};

mod select_table_stats;
pub use self::select_table_stats::select_table_stats;
pub(crate) use self::select_table_stats::TableStats;

mod select_table_stats_cache;
pub(crate) use self::select_table_stats_cache::select_all_table_stats;

mod update_table_rows;
pub use update_table_rows::{update_table_rows, UpdateParams};

mod postgres_types;

mod utils;

use postgres_types::RowValues;
use serde::Serialize;

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    QueryTableResult(Vec<RowValues>),
    RowsAffected { num_rows: u64 },
}

impl QueryResult {
    pub fn from_num_rows_affected(num_rows: u64) -> Self {
        QueryResult::RowsAffected { num_rows }
    }
}
