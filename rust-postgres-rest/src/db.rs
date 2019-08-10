use crate::error::Error;
use actix::spawn;
use futures::Future;
use tokio_postgres::{connect as pg_connect, Client, NoTls};

/// Returns a future that evaluates to the database client connection.
pub fn connect(db_url: &str) -> impl Future<Item = Client, Error = Error> {
    pg_connect(db_url, NoTls)
        .map_err(Error::from)
        .and_then(|(client, connection)| {
            spawn(connection.map_err(|e| panic!("{}", e)));
            Ok(client)
        })
}
