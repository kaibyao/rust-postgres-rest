use super::{
    postgres_types::{convert_row_fields, ColumnTypeValue, RowFields},
    query_types::{QueryParamsUpdate, QueryResult},
    select_table_stats::{select_column_stats, select_column_stats_statement},
};
use crate::{db::connect, AppState, Error};
use futures::{
    future::{err, loop_fn, Either, Future, Loop},
    stream::Stream,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use tokio_postgres::{types::ToSql, Client};

enum UpdateResult {
    Rows(Vec<RowFields>),
    NumRowsAffected(u64),
}

/// Runs an UPDATE query on the selected table rows
pub fn update_table_rows(
    state: &AppState,
    params: QueryParamsUpdate,
) -> impl Future<Item = QueryResult, Error = Error> {
    err(Error::generate_error("TEST", "".to_string()))
}
