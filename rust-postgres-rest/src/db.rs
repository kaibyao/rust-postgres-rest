use crate::error::Error;
use actix::spawn;
use futures::Future;
use tokio_postgres::{connect as pg_connect, Client, NoTls};

// todo: Tls option in config builder, connect should use config option

/// A convenience wrapper around `tokio_postgres::connect`. Returns a future that evaluates to the
/// database client connection.
///
/// # Example
///
/// ```no_run
/// use futures::future::{Future, ok};
/// use futures::stream::Stream;
/// use rust_postgres_rest::{connect, Error};
///
/// let fut = connect("postgresql://postgres@0.0.0.0:5432/postgres")
///     .map_err(|e| panic!(e))
///     .and_then(|mut _client| {
///         // do something with the db client
///         ok(())
///     });
///
/// tokio::run(fut);
/// ```
pub fn connect(db_url: &str) -> impl Future<Item = Client, Error = Error> {
    pg_connect(db_url, NoTls)
        .map_err(Error::from)
        .and_then(|(client, connection)| {
            spawn(connection.map_err(|e| panic!("{}", e)));
            Ok(client)
        })
}
