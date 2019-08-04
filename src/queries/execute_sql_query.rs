use super::{
    postgres_types::{convert_row_fields, RowFields},
    query_types::{QueryParamsExecute, QueryResult},
};
use crate::Error;
use futures::{
    future::{Either, Future},
    stream::Stream,
};
use tokio_postgres::Client;

pub fn execute_sql_query(
    mut client: Client,
    params: QueryParamsExecute,
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
                        let convert_row_result: Result<Vec<RowFields>, Error> =
                            rows.iter().map(convert_row_fields).collect();

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
