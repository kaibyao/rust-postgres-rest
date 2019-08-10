use crate::error::Error;
use future::Future;
use futures::{future, stream::Stream};
use tokio_postgres::Client;

/// Retrieves all user-created table names
pub fn select_all_tables(
    mut client: Client,
) -> impl Future<Item = (Vec<String>, Client), Error = Error> {
    let statement_str = "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name;";

    client
        .prepare(statement_str)
        .map_err(Error::from)
        .and_then(move |statement| {
            client
                .query(&statement, &[])
                .map_err(Error::from)
                .map(|row| row.get(0))
                .collect()
                .map(move |tables| (tables, client))
        })
}
