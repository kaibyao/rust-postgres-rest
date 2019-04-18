mod index;
pub use index::index;

mod table;
pub use table::{get_all_table_names, insert_into_table, query_table};
