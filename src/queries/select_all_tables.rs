use crate::db::Connection;
use future::{err, Either, Future};
use futures::future;
use futures::stream::Stream;
use tokio_postgres::Error as PgError;

/// Retrieves all user-created table names
pub fn select_all_tables(mut conn: Connection) -> impl Future<Item = Vec<String>, Error = PgError> {
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";

    conn.client
        .prepare(statement_str)
        .then(move |result| match result {
            Ok(statement) => {
                let select_all_tables_future = conn
                    .client
                    .query(&statement, &[])
                    .map(|row| row.get(0))
                    .collect();

                Either::A(select_all_tables_future)
            }
            Err(e) => Either::B(err(e)),
        })
}
