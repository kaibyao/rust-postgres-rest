// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

mod error;

/// Contains the functions used to query the database.
pub mod queries;

mod stats_cache;
use stats_cache::{get_stats_cache_addr, StatsCacheMessage};

pub use error::Error;

use actix::{spawn as actix_spawn, System};
use futures::future::{err, ok, Either, Future};
use tokio::runtime::current_thread::TaskExecutor;
use tokio_postgres::{connect as pg_connect, tls::MakeTlsConnect, Client, Socket};

/// Configures the DB connection and API.
#[derive(Clone)]
pub struct Config<T>
where
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    /// The database URL. URL must be [Postgres-formatted](https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6).
    pub db_url: &'static str,
    /// When set to `true`, caching of table stats is enabled, significantly speeding up API
    /// endpoings that use `SELECT` and `INSERT` statements. Default: `false`.
    pub is_cache_table_stats: bool,
    /// When set to a positive integer `n`, automatically refresh the Table Stats cache every `n`
    /// seconds. Default: `0` (cache is never automatically reset).
    pub cache_reset_interval_seconds: u32,
    /// A Tls connection that can be passed into `tokio_postgres::connect`.
    tls: T,
}

impl<T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static> Config<T> {
    /// Creates a new Config.
    /// ```
    /// use postgres_rest::Config;
    /// use tokio_postgres::tls::NoTls;
    ///
    /// let mut config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", NoTls);
    /// ```
    pub fn new(db_url: &'static str, tls: T) -> Self {
        Config {
            db_url,
            is_cache_table_stats: false,
            cache_reset_interval_seconds: 0,
            tls,
        }
    }

    /// Turns on the flag for caching table stats. Substantially increases performance. Use this in
    /// production or in systems where the DB schema is not changing.
    pub fn cache_table_stats(&mut self) -> &mut Self {
        self.is_cache_table_stats = true;
        stats_cache::initialize_stats_cache(self);
        self
    }

    /// A convenience wrapper around `tokio_postgres::connect`. Returns a future that evaluates to
    /// the database client connection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use futures::future::{Future, ok};
    /// use futures::stream::Stream;
    /// use postgres_rest::{Config};
    /// use tokio_postgres::tls::NoTls;
    ///
    /// let mut config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", NoTls);
    ///
    /// let fut = config.connect()
    ///     .map_err(|e| panic!(e))
    ///     .and_then(|mut _client| {
    ///         // do something with the db client
    ///         ok(())
    ///     });
    ///
    /// tokio::run(fut);
    /// ```
    pub fn connect(&self) -> impl Future<Item = Client, Error = Error> {
        pg_connect(self.db_url, self.tls.clone())
            .map_err(Error::from)
            .and_then(|(client, connection)| {
                let is_actix_result = std::panic::catch_unwind(|| {
                    System::current();
                });

                if is_actix_result.is_ok() {
                    actix_spawn(connection.map_err(|e| panic!("{}", e)));
                } else {
                    let _spawn_result = TaskExecutor::current()
                        .spawn_local(Box::new(connection.map_err(|e| panic!("{}", e))));
                }

                Ok(client)
            })
    }

    /// Forces the Table Stats cache to reset/refresh new data.
    pub fn reset_cache(&self) -> impl Future<Item = (), Error = Error> {
        if !self.is_cache_table_stats {
            return Either::A(err(Error::generate_error(
                "TABLE_STATS_CACHE_NOT_ENABLED",
                "".to_string(),
            )));
        }

        match get_stats_cache_addr() {
            Some(addr) => {
                let reset_cache_future = addr
                    .send(StatsCacheMessage::ResetCache)
                    .map_err(Error::from)
                    .and_then(|response_result| match response_result {
                        Ok(_response_ok) => ok(()),
                        Err(e) => err(e),
                    });
                Either::B(reset_cache_future)
            }
            None => Either::A(err(Error::generate_error(
                "TABLE_STATS_CACHE_NOT_INITIALIZED",
                "The cache to be reset was not found.".to_string(),
            ))),
        }
    }

    /// Set the interval timer to automatically reset the table stats cache. If this is not set, the
    /// cache is never reset.
    /// ```
    /// use postgres_rest::Config;
    /// use tokio_postgres::tls::NoTls;
    ///
    /// let mut config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", NoTls);
    /// config.set_cache_reset_timer(300); // Cache will refresh every 5 minutes.
    /// ```
    pub fn set_cache_reset_timer(&mut self, seconds: u32) -> &mut Self {
        self.cache_reset_interval_seconds = seconds;
        self
    }
}

#[cfg(test)]
mod tls_tests {
    use super::*;

    use native_tls::{Certificate, TlsConnector};
    use postgres_native_tls::MakeTlsConnector;
    use std::fs;
    use tokio_postgres::NoTls;

    #[test]
    fn no_tls() {
        let config = Config::new("postgresql://postgres:example@0.0.0.0:5433/postgres", NoTls);
        config.connect().wait().unwrap();
    }

    #[test]
    fn native_tls() {
        let cert_str = fs::read("./tests/server.pem").unwrap();
        let cert = Certificate::from_pem(&cert_str).unwrap();
        let tls_connector = TlsConnector::builder()
            .add_root_certificate(cert)
            .build()
            .unwrap();
        let postgres_tls_connector = MakeTlsConnector::new(tls_connector);

        let cfg = Config::new("host=localhost port=5433 user=postgres password=example dbname=postgres sslmode=require", postgres_tls_connector);
        cfg.connect().wait().unwrap();
    }
}
