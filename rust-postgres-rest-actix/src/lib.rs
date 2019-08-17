// nightly features
#![feature(async_await)]
// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

//! Use `actix-web` to serve a REST API for your PostgreSQL database.
//!
//! # Example
//!
//! ```
//! use actix_web::{App, HttpServer};
//! use rust_postgres_rest_actix::{Config};
//! use tokio_postgres::tls::NoTls;
//! # use std::thread;
//!
//! fn main() {
//!     let ip_address = "127.0.0.1:3000";
//!
//!     // start 1 server on each cpu thread
//!     # thread::spawn(move || {
//!     HttpServer::new(move || {
//!         App::new().service(
//!             // appends an actix-web Scope under the "/api" endpoint to app.
//!             Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls)
//!                 .generate_scope("/api"),
//!         )
//!     })
//!     .bind(ip_address)
//!     .expect("Can not bind to port 3000")
//!     .run()
//!     .unwrap();
//!     # });
//!
//!     println!("Running server on {}", ip_address);
//! }
//! ```

/// Individual endpoints that can be applied to actix routes using `.to_async()`.
///
/// The `Config` object must be saved to the resource using `.data()`, and the route must have a
/// `"table"` query parameter.
///
/// # Example
///
/// ```
/// use actix_web::{App, HttpServer, web};
/// use rust_postgres_rest_actix::{Config, endpoints};
/// use tokio_postgres::tls::NoTls;
///
/// fn main() {
///     let ip_address = "127.0.0.1:3000";
///
///     // start 1 server on each cpu thread
///     # std::thread::spawn(move || {
///     HttpServer::new(move || {
///         let config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls);
///
///         App::new().service(
///             web::scope("/custom_api_endpoint")
///                 .data(config)
///                 .route("/table", web::get().to_async(endpoints::get_all_table_names::<NoTls>))
///                 .service(
///                     web::resource("/{table}")
///                         .route(web::get().to_async(endpoints::get_table::<NoTls>))
///                         .route(web::post().to_async(endpoints::post_table::<NoTls>))
///                 )
///         )
///     })
///     .bind(ip_address)
///     .expect("Can not bind to port 3000")
///     .run()
///     .unwrap();
///     # });
///
///     println!("Running server on {}", ip_address);
/// }
/// ```
pub mod endpoints;

mod error;

use endpoints::{
    delete_table, execute_sql, get_all_table_names, get_table, index, post_table, put_table,
    reset_caches,
};

pub use error::Error;
use rust_postgres_rest::Config as InnerConfig;

use actix_web::{web, Scope};
use futures::future::Future;
use tokio_postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Client, Socket,
};

/// Configures and creates the REST API `Scope`.
/// ```
/// use rust_postgres_rest_actix::Config;
/// use tokio_postgres::tls::NoTls;
///
/// let config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls);
/// let scope = config.generate_scope("/api");
/// ```
#[derive(Clone)]
pub struct Config<T>
where
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    inner: InnerConfig<T>,
    /// When set to `true`, an additional API endpoint is made available at
    /// `{scope_name}/reset_table_stats_cache`, which allows for manual resetting of the Table
    /// Stats cache. This is useful if you want a persistent cache that only needs to be reset on
    /// upgrades, for example. Default: `false`.
    is_cache_reset_endpoint_enabled: bool,
    /// When set to `true`, an additional API endpoint is made available at `{scope_name}/sql`,
    /// which allows for custom SQL queries to be executed. Default: `false`.
    is_custom_sql_endpoint_enabled: bool,
}

impl<T> Config<T>
where
    <T as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <T as MakeTlsConnect<Socket>>::Stream: Send,
    <<T as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
{
    /// Creates a Config object with default values. `db_url` must be [Postgres-formatted](https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6).
    /// ```
    /// use rust_postgres_rest_actix::Config;
    /// use tokio_postgres::tls::NoTls;
    ///
    /// let config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls);
    /// ```
    pub fn new(db_url: &'static str, tls: fn() -> T) -> Self {
        Config {
            inner: InnerConfig::new(db_url, tls),
            is_cache_reset_endpoint_enabled: false,
            is_custom_sql_endpoint_enabled: false,
        }
    }

    /// Turns on the flag for caching table stats. Substantially increases performance. Use this in
    /// production or in systems where the DB schema is not changing.
    pub fn cache_table_stats(&mut self) -> &mut Self {
        self.inner.cache_table_stats();
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
    /// use rust_postgres_rest_actix::{Config};
    /// use tokio_postgres::tls::NoTls;
    ///
    /// actix::run(|| Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls).connect()
    ///     .map_err(|e| panic!(e))
    ///     .and_then(|mut _client| {
    ///         // do something with the db client
    ///         ok(())
    ///     }));
    /// ```
    pub fn connect(&self) -> impl Future<Item = Client, Error = Error> {
        self.inner.connect().map_err(Error::from)
    }

    /// Enables an additional API endpoint at `{scope_name}/reset_table_stats_cache`, which allows
    /// for manual resetting of the Table Stats cache.
    pub fn enable_cache_reset_url(&mut self) -> &mut Self {
        self.is_cache_reset_endpoint_enabled = true;
        self
    }

    /// Enables an additional API endpoint at `{scope_name}/sql`, which allows for custom SQL
    /// queries to be executed.
    pub fn enable_custom_sql_url(&mut self) -> &mut Self {
        self.is_custom_sql_endpoint_enabled = true;
        self
    }

    /// Creates the Actix scope url at `scope_name`, which contains all of the other API endpoints.
    /// ```
    /// use rust_postgres_rest_actix::Config;
    /// use tokio_postgres::tls::NoTls;
    ///
    /// let config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls);
    /// let scope = config.generate_scope("/api");
    /// ```
    pub fn generate_scope(&self, scope_name: &str) -> Scope {
        let mut scope = web::scope(scope_name);

        if self.inner.is_cache_table_stats {
            scope = scope.route(
                "/reset_table_stats_cache",
                // web::get().to_async(typed_reset_caches),
                web::get().to_async(reset_caches::<T>),
            );
        }

        if self.is_custom_sql_endpoint_enabled {
            scope = scope.route("/sql", web::post().to_async(execute_sql::<T>));
        }

        scope
            .data(self.clone())
            .route("", web::get().to(index))
            .route("/", web::get().to(index))
            .route("/table", web::get().to_async(get_all_table_names::<T>))
            .service(
                web::resource("/{table}")
                    .route(web::delete().to_async(delete_table::<T>))
                    .route(web::get().to_async(get_table::<T>))
                    .route(web::post().to_async(post_table::<T>))
                    .route(web::put().to_async(put_table::<T>)),
            )
    }

    /// Set the timer to automatically reset the table stats cache on a recurring interval. If this
    /// is not set, the cache is never reset after server start.
    /// ```
    /// use rust_postgres_rest_actix::Config;
    /// use tokio_postgres::tls::NoTls;
    ///
    /// let mut config = Config::new("postgresql://postgres@0.0.0.0:5432/postgres", || NoTls);
    /// config.set_cache_reset_timer(300); // Cache will refresh every 5 minutes.
    /// ```
    pub fn set_cache_reset_timer(&mut self, seconds: u32) -> &mut Self {
        self.inner.set_cache_reset_timer(seconds);
        self
    }
}
