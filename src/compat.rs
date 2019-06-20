use chrono::*;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::time::{SystemTime};
use rust_decimal::Decimal;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

pub(crate) trait ToSqlSyncSend: Sync + Send + ToSql {}
impl ToSql for dyn ToSqlSyncSend {}

// primitives

impl<'a, T> ToSqlSyncSend for &'a T
where
    T: ToSqlSyncSend,
{}

impl<T: ToSqlSyncSend> ToSqlSyncSend for Option<T> {}

impl<'a, T: ToSqlSyncSend> ToSqlSyncSend for &'a [T] {}

impl<'a> ToSqlSyncSend for &'a [u8] {}

impl<T: ToSqlSyncSend> ToSqlSyncSend for Vec<T> {}

impl ToSqlSyncSend for Vec<u8> {}

impl<'a> ToSqlSyncSend for &'a str {}

impl<'a> ToSqlSyncSend for Cow<'a, str> {}

impl ToSqlSyncSend for String {}

macro_rules! simple_to {
    ($t:ty) => {
        impl ToSqlSyncSend for $t {}
    }
}

simple_to!(bool);
simple_to!(i8);
simple_to!(i16);
simple_to!(i32);
simple_to!(u32);
simple_to!(i64);
simple_to!(f32);
simple_to!(f64);

impl<H> ToSqlSyncSend for HashMap<String, Option<String>, H>
where
    H: BuildHasher,
{}

impl ToSqlSyncSend for SystemTime {}

// chrono

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)
}

impl ToSqlSyncSend for NaiveDateTime {}
impl ToSqlSyncSend for DateTime<Utc> {}
impl ToSqlSyncSend for DateTime<Local> {}
impl ToSqlSyncSend for DateTime<FixedOffset> {}
impl ToSqlSyncSend for NaiveDate {}
impl ToSqlSyncSend for NaiveTime {}

// other types

impl ToSqlSyncSend for Decimal {}
impl ToSqlSyncSend for Uuid {}
impl ToSqlSyncSend for Value {}
