mod index;
pub(crate) use index::index;

mod table;
pub use table::{get_all_table_names, get_table, post_table};
