use future::{err, Either, Future};
use futures::future;
use futures::stream::Stream;

use tokio_postgres::{Client, Error};

/// Retrieves all user-created table names
pub fn get_all_tables(
    mut client: Client,
) -> impl Future<Item = (Vec<String>, Client), Error = (Error, Client)> {
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";

    client.prepare(statement_str).then(|result| match result {
        Ok(statement) => {
            let f = client
                .query(&statement, &[])
                .map(|row| row.get(0))
                .collect()
                .then(move |result: Result<Vec<String>, Error>| match result {
                    Ok(rows) => Ok((rows, client)),
                    Err(e) => Err((e, client)),
                });

            Either::A(f)
        }
        Err(e) => Either::B(err((e, client))),
    })
}
