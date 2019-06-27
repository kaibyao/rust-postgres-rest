use actix::spawn;
use futures::Future;
use tokio_postgres::{connect as pg_connect, Client, Error as PgError, NoTls};

/// Initializes the database connection pool and returns it.
pub fn connect(db_url: &str) -> impl Future<Item = Client, Error = PgError> {
    pg_connect(db_url, NoTls).and_then(|(client, connection)| {
        spawn(connection.map_err(|e| panic!("{}", e)));
        Ok(client)
    })
}
