// nightly features
#![feature(async_await)]
// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

/// Individual endpoints that can be applied to actix routes using `.to_async()`.
pub mod endpoints;

mod error;

use endpoints::index;
pub use endpoints::{
    delete_table, execute_sql, get_all_table_names, get_table, post_table, put_table, reset_caches,
};

pub use error::Error;
use rust_postgres_rest::Config as InnerConfig;

// use actix::Addr;
use actix_web::{web, Scope};

/// API Configuration
#[derive(Clone)]
pub struct Config {
    inner: InnerConfig,
    /// When set to `true`, an additional API endpoint is made available at
    /// `{scope_name}/reset_table_stats_cache`, which allows for manual resetting of the Table
    /// Stats cache. This is useful if you want a persistent cache that only needs to be reset on
    /// upgrades, for example. Default: `false`.
    pub is_cache_reset_endpoint_enabled: bool,
    /// When set to `true`, an additional API endpoint is made available at `{scope_name}/sql`,
    /// which allows for custom SQL queries to be executed. Default: `false`.
    pub is_custom_sql_endpoint_enabled: bool,
}

impl Config {
    /// Creates a Config object with default values.
    pub fn new(db_url: &'static str) -> Self {
        Config {
            inner: InnerConfig::new(db_url),
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
    pub fn generate_scope(&mut self, scope_name: &str) -> Scope {
        let mut scope = web::scope(scope_name);

        if self.inner.is_cache_table_stats {
            scope = scope.route(
                "/reset_table_stats_cache",
                web::get().to_async(reset_caches),
            );
        }

        if self.is_custom_sql_endpoint_enabled {
            scope = scope.route("/sql", web::post().to_async(execute_sql));
        }

        scope
            .data(self.clone())
            .route("", web::get().to(index))
            .route("/", web::get().to(index))
            .route("/table", web::get().to_async(get_all_table_names))
            .service(
                web::resource("/{table}")
                    .route(web::delete().to_async(delete_table))
                    .route(web::get().to_async(get_table))
                    .route(web::post().to_async(post_table))
                    .route(web::put().to_async(put_table)),
            )
    }

    // Set the interval timer to automatically reset the table stats cache. If this is not set, the
    // cache is never reset.
    pub fn set_cache_reset_timer(&mut self, seconds: u32) -> &mut Self {
        self.inner.set_cache_reset_timer(seconds);
        self
    }
}
