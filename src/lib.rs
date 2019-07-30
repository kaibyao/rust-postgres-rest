// nightly features
#![feature(async_await)]
// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

/// Individual endpoints that can be applied to actix routes using `.to_async()`.
pub mod endpoints;

mod db;
mod error;
mod queries;
#[cfg(feature = "stats_cache")]
mod stats_cache;
use endpoints::{get_all_table_names, get_table, index, post_table, put_table, reset_caches};

pub use error::Error;
use stats_cache::initialize_stats_cache;

use actix::Addr;
use actix_web::{web, Scope};

/// API Configuration
#[derive(Clone)]
pub struct AppConfig {
    /// The database URL. URL must be [Postgres-formatted](https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6).
    pub db_url: &'static str,
    /// Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When
    /// set to `true`, caching of table stats is enabled, significantly speeding up API endpoings
    /// that use `SELECT` and `INSERT` statements. Default: `false`.
    #[cfg(feature = "stats_cache")]
    pub is_cache_table_stats: bool,
    /// Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When
    /// set to `true`, an additional API endpoint is made available at
    /// `{scope_name}/reset_table_stats_cache`, which allows for manual resetting of the Table
    /// Stats cache. This is useful if you want a persistent cache that only needs to be reset on
    /// upgrades, for example. Default: `false`.
    pub is_cache_reset_endpoint_enabled: bool,
    /// Requires the `stats_cache` cargo feature to be enabled (which is enabled by default). When
    /// set to a positive integer `n`, automatically refresh the Table Stats cache every `n`
    /// seconds. Default: `0` (cache is never automatically reset).
    pub cache_reset_interval_seconds: u32,
    /// The API endpoint that contains all of the other API operations available in this library.
    pub scope_name: &'static str,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            db_url: "",
            is_cache_table_stats: false,
            is_cache_reset_endpoint_enabled: false,
            cache_reset_interval_seconds: 0,
            scope_name: "/api",
        }
    }
}
impl AppConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Contains information about the current state of the API. Used by `generate_rest_api_scope()`.
/// This only needs to be used if API endpoints are being manually set up.
pub struct AppState {
    /// AppConfig object.
    pub config: AppConfig,
    /// Actor address for the Table Stats Cache.
    stats_cache_addr: Option<Addr<stats_cache::StatsCache>>,
}

impl AppState {
    /// Creates a new `AppState`.
    pub fn new(config: AppConfig) -> Self {
        AppState {
            config,
            stats_cache_addr: None,
        }
    }
}

/// Creates and returns an actix scope containing the REST API endpoints.
pub fn generate_rest_api_scope(config: AppConfig) -> Scope {
    let mut state = AppState::new(config);

    let mut scope = web::scope(state.config.scope_name);

    if state.config.is_cache_table_stats {
        initialize_stats_cache(&mut state);
        scope = scope.route(
            "/reset_table_stats_cache",
            web::get().to_async(reset_caches),
        );
    }

    scope
        .data(state)
        .route("", web::get().to(index))
        .route("/", web::get().to(index))
        .route("/table", web::get().to_async(get_all_table_names))
        .service(
            web::resource("/{table}")
                .route(web::get().to_async(get_table))
                .route(web::post().to_async(post_table))
                .route(web::put().to_async(put_table)),
        )
}
