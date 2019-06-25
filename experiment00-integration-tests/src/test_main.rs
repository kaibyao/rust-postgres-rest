#![feature(async_await)]

mod setup;

use actix::System;
use futures::future::lazy;
use futures03::{FutureExt, TryFutureExt};
// use futures03::future::ok;
// use futures03::compat::Future01CompatExt;

static DB_URL: &str = "postgresql://postgres:example@0.0.0.0:5433/postgres";

#[test]
fn run_integration_tests() {
    let mut sys = System::new("integration_tests");

    // setup
    sys.block_on(setup::setup_db(DB_URL).boxed().compat())
        .unwrap();
    setup::start_server(DB_URL);

    // run tests


    // shut down
    sys.block_on(lazy(|| -> Result<(), ()> {
        System::current().stop();
        Ok(())
    }))
    .unwrap();

    sys.run().unwrap();
}
