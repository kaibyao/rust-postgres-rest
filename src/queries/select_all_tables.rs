use future::{err, Either, Future};
use futures::{future, stream::Stream};
use tokio_postgres::{Client, Error as PgError};

/// Retrieves all user-created table names
pub fn select_all_tables(
    mut client: Client,
) -> impl Future<Item = (Vec<String>, Client), Error = (PgError, Client)> {
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";

    client
        .prepare(statement_str)
        .then(move |result| match result {
            Ok(statement) => {
                let select_all_tables_future = client
                    .query(&statement, &[])
                    .map(|row| row.get(0))
                    .collect()
                    .then(|result| match result {
                        Ok(rows) => Ok((rows, client)),
                        Err(e) => Err((e, client)),
                    });

                Either::A(select_all_tables_future)
            }
            Err(e) => Either::B(err((e, client))),
        })
}
