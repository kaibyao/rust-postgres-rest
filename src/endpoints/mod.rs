mod index;
pub use index::index;

mod table;
pub use table::{create_table, get_all_table_names, query_table};
