// nightly features
#![feature(async_await)]
// used for dev/tests
#![deny(clippy::complexity, clippy::correctness, clippy::perf, clippy::style)]
// to serialize large json (like the index)
#![recursion_limit = "128"]

/// Provides endpoints that can be used by `actix-web` to serve a REST API for a PostgreSQL
/// database.

/// Individual endpoints that can be applied to actix routes using `.to_async()`.
pub mod endpoints;

mod db;
mod error;
mod queries;
#[cfg(feature = "stats_cache")]
mod stats_cache;
use endpoints::{get_all_table_names, get_table, index, post_table};

pub use error::Error;
use stats_cache::initialize_stats_cache;

use actix::Addr;
use actix_web::{web, Scope};

/// API Configuration
#[derive(Clone)]
pub struct AppConfig {
    /// postgres-formatted URL.
    pub db_url: &'static str,
    /// Table stats are retrieved when querying for foreign keys and on every INSERT operation.
    /// Turning this on is suggested for production systems, as there is a noticeable improvement
    /// to performance. Default: `false`.
    #[cfg(feature = "stats_cache")]
    pub is_cache_table_stats: bool,
    /// Only applies when `is_cache_table_stats`. Enables another endpoint at `/stats_cache_reset`
    /// that refreshes the table stats cache. Default: `false`.
    pub is_cache_reset_endpoint_enabled: bool,
    /// Only applies when `is_cache_table_stats`. The amount of time in seconds that elapses
    /// before the table stats cache automatically refreshes. Setting to `0` means it never
    /// refreshes. Default: `0`.
    pub cache_reset_interval_seconds: u32,
    /// The API endpoint that contains all of the table operation endpoints.
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

    if state.config.is_cache_table_stats {
        initialize_stats_cache(&mut state);
    }

    web::scope(state.config.scope_name)
        .data(state)
        .route("", web::get().to(index))
        .route("/", web::get().to(index))
        .route("/table", web::get().to_async(get_all_table_names))
        .service(
            web::resource("/{table}")
                .route(web::get().to_async(get_table))
                .route(web::post().to_async(post_table)),
        )
}
