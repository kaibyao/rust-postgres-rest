use super::{
    postgres_types::{row_to_row_values, RowValues},
    QueryResult,
};
use crate::Error;
use futures::{
    future::{Either, Future},
    stream::Stream,
};
use rayon::prelude::*;
use tokio_postgres::Client;

#[derive(Debug)]
/// Options used to execute a custom SQL query.
pub struct ExecuteParams {
    pub statement: String,
    pub is_return_rows: bool,
}

/// Executes an SQL query statement.
pub fn execute_sql_query(
    mut client: Client,
    params: ExecuteParams,
) -> impl Future<Item = QueryResult, Error = Error> {
    client
        .prepare(&params.statement)
        .map_err(Error::from)
        .and_then(move |statement| {
            if params.is_return_rows {
                let rows_future = client
                    .query(&statement, &[])
                    .map_err(Error::from)
                    .collect()
                    .and_then(|rows| {
                        let convert_row_result: Result<Vec<RowValues>, Error> =
                            rows.par_iter().map(row_to_row_values).collect();

                        convert_row_result
                    })
                    .map(QueryResult::QueryTableResult);

                Either::A(rows_future)
            } else {
                let num_rows_future = client
                    .execute(&statement, &[])
                    .map_err(Error::from)
                    .map(QueryResult::from_num_rows_affected);

                Either::B(num_rows_future)
            }
        })
}
