use crate::queries::query_types::QueryResult;
use crate::errors::ApiError;
use futures::future::Future;
use futures::stream::Stream;
use tokio_postgres::{Client};

/// Retrieves all user-created table names
pub fn get_all_tables(
    mut conn: Client,
) -> impl Future<Item = QueryResult, Error = ApiError> + 'static {
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";

    conn.prepare(statement_str)
        .map(move |statement| (conn, statement))
        .and_then(|(mut conn, statement)| conn.query(&statement, &[]).collect())
        .map_err(|e| ApiError::from(e))
        .map(|rows| {
            QueryResult::GetAllTablesResult(rows.iter().map(|r| r.get(0)).collect())
        })
}
