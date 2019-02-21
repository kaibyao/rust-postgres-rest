use crate::db::Connection;
use crate::errors::ApiError;

use super::query_types::QueryResult;

/// Retrieves all user-created table names
pub fn get_all_tables(conn: &Connection) -> Result<QueryResult, ApiError> {
    let statement = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";
    let prep_statement = conn.prepare(statement)?;

    let results: Vec<String> = prep_statement
        .query(&[])?
        .into_iter()
        .map(|row| row.get(0))
        .collect();

    Ok(QueryResult::GetAllTablesResult(results))
}
