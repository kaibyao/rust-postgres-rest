use crate::queries::query_types::QueryResult;
use crate::errors::ApiError;
use futures::future::Future;
use futures::stream::Stream;
use tokio_postgres::{Client, Error};

/// Retrieves all user-created table names
pub fn get_all_tables(
    mut client: Client,
) -> impl Future<Item = Vec<String>, Error = ApiError> {
// ) -> impl Future<Item = QueryResult, Error = ApiError> { // using actors
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";

    client.prepare(statement_str)
        .map(move |statement| (client, statement))
        .and_then(|(mut cl, statement)| cl.query(&statement, &[]).collect())
        .map(|rows| {
            // QueryResult::GetAllTablesResult(rows.iter().map(|r| r.get(0)).collect())
            rows.iter().map(|r| r.get(0)).collect()
        })
        .map_err(ApiError::from)
}
