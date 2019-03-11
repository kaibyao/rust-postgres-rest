use super::query_types::{Query, QueryResult};
use crate::db::Connection;
use crate::errors::ApiError;

pub fn create_table(conn: &Connection, query: Query) -> Result<QueryResult, ApiError> {
    Ok(QueryResult::Success(true))
}
