#![feature(async_await)]
#![recursion_limit = "128"]

mod setup;
use setup::{setup_db, start_web_server};

use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use reqwest::{self, Client, Method, StatusCode};
use serde_json::{self, json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Once,
};

lazy_static! {
    static ref SERVER_IP: &'static str = "127.0.0.1";
    static ref NO_CACHE_PORT: &'static str = "8000";
    static ref CACHE_PORT: &'static str = "8001";
    static ref DB_URL: &'static str = "postgresql://postgres:example@0.0.0.0:5433/postgres";
    static ref IS_RAN_SETUP: AtomicBool = AtomicBool::new(false);
    static ref RUN_SETUP: Once = Once::new();
}

fn run_setup() {
    if !IS_RAN_SETUP.load(Ordering::SeqCst) {
        RUN_SETUP.call_once(|| {
            println!("Setting up database...");
            setup_db(&DB_URL);

            println!("starting webserver...");
            start_web_server(&DB_URL, &SERVER_IP);

            IS_RAN_SETUP.store(true, Ordering::SeqCst);
        });
    }
}

#[test]
fn index() {
    run_setup();

    let url = ["http://", &SERVER_IP, ":", &NO_CACHE_PORT, "/api/"].join("");
    let res = reqwest::get(&url).unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn index_no_slash() {
    run_setup();

    let url = ["http://", &SERVER_IP, ":", &NO_CACHE_PORT, "/api"].join("");
    let res = reqwest::get(&url).unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_names() {
    run_setup();

    let url = ["http://", &SERVER_IP, ":", &NO_CACHE_PORT, "/api/table"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        response_body,
        json!([
            "adult",
            "child",
            "coach",
            "company",
            "delete_a",
            "delete_b",
            "delete_simple",
            "player",
            "school",
            "sibling",
            "team",
            "test_batch_insert",
            "test_fields",
            "test_insert",
        ])
    );
}

#[test]
fn get_table_stats() {
    run_setup();

    let expected_response_body = json!({"columns":[{"char_max_length":null,"char_octet_length":null,"column_name":"school_id","column_type":"int8","default_value":null,"foreign_key_column":"id","foreign_key_column_type":"int8","foreign_key_table":"school","is_foreign_key":true,"is_nullable":true},{"char_max_length":null,"char_octet_length":null,"column_name":"parent_id","column_type":"int8","default_value":null,"foreign_key_column":"id","foreign_key_column_type":"int8","foreign_key_table":"adult","is_foreign_key":true,"is_nullable":true},{"char_max_length":null,"char_octet_length":1_073_741_824,"column_name":"name","column_type":"text","default_value":null,"foreign_key_column":null,"foreign_key_column_type":null,"foreign_key_table":null,"is_foreign_key":false,"is_nullable":true},{"char_max_length":null,"char_octet_length":null,"column_name":"id","column_type":"int8","default_value":null,"foreign_key_column":null,"foreign_key_column_type":null,"foreign_key_table":null,"is_foreign_key":false,"is_nullable":false}],"constraints":[{"columns":["id"],"constraint_type":"primary_key","definition":"PRIMARY KEY (id)","fk_columns":null,"fk_table":null,"name":"child_id_key","table":"child"},{"columns":["parent_id"],"constraint_type":"foreign_key","definition":"FOREIGN KEY (parent_id) REFERENCES adult(id)","fk_columns":["id"],"fk_table":"adult","name":"child_parent_id","table":"child"},{"columns":["school_id"],"constraint_type":"foreign_key","definition":"FOREIGN KEY (school_id) REFERENCES school(id)","fk_columns":["id"],"fk_table":"school","name":"child_school_id","table":"child"},{"columns":["id","parent_id"],"constraint_type":"unique","definition":"UNIQUE (id, parent_id)","fk_columns":null,"fk_table":null,"name":"child_unique_id_parent_id","table":"child"},{"columns":["parent_id","sibling_id"],"constraint_type":"foreign_key","definition":"FOREIGN KEY (parent_id, sibling_id) REFERENCES child(parent_id, id)","fk_columns":["parent_id","id"],"fk_table":"child","name":"sibling_reference","table":"sibling"}],"indexes":[{"access_method":"btree","columns":["id"],"is_exclusion":false,"is_primary_key":true,"is_unique":true,"name":"child_id_key"},{"access_method":"btree","columns":["id","parent_id"],"is_exclusion":false,"is_primary_key":false,"is_unique":true,"name":"child_unique_id_parent_id"}],"primary_key":["id"],"referenced_by":[{"columns_referenced":["parent_id","id"],"referencing_columns":["parent_id","sibling_id"],"referencing_table":"sibling"}]});

    // test the non-cached path
    let url = ["http://", &SERVER_IP, ":", &NO_CACHE_PORT, "/api/child"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);

    // test the cached path
    let url = ["http://", &SERVER_IP, ":", &CACHE_PORT, "/api/child"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);
}

#[test]
fn get_table_stats_multi_column_fk() {
    run_setup();

    let expected_response_body = json!({"columns":[{"char_max_length":null,"char_octet_length":null,"column_name":"sibling_id","column_type":"int8","default_value":null,"foreign_key_column":"id","foreign_key_column_type":"int8","foreign_key_table":"child","is_foreign_key":true,"is_nullable":true},{"char_max_length":null,"char_octet_length":null,"column_name":"parent_id","column_type":"int8","default_value":null,"foreign_key_column":"parent_id","foreign_key_column_type":"int8","foreign_key_table":"child","is_foreign_key":true,"is_nullable":true},{"char_max_length":null,"char_octet_length":1_073_741_824,"column_name":"name","column_type":"text","default_value":null,"foreign_key_column":null,"foreign_key_column_type":null,"foreign_key_table":null,"is_foreign_key":false,"is_nullable":true},{"char_max_length":null,"char_octet_length":null,"column_name":"id","column_type":"int8","default_value":null,"foreign_key_column":null,"foreign_key_column_type":null,"foreign_key_table":null,"is_foreign_key":false,"is_nullable":false}],"constraints":[{"columns":["id"],"constraint_type":"primary_key","definition":"PRIMARY KEY (id)","fk_columns":null,"fk_table":null,"name":"sibling_id_key","table":"sibling"},{"columns":["parent_id","sibling_id"],"constraint_type":"foreign_key","definition":"FOREIGN KEY (parent_id, sibling_id) REFERENCES child(parent_id, id)","fk_columns":["parent_id","id"],"fk_table":"child","name":"sibling_reference","table":"sibling"}],"indexes":[{"access_method":"btree","columns":["id"],"is_exclusion":false,"is_primary_key":true,"is_unique":true,"name":"sibling_id_key"}],"primary_key":["id"],"referenced_by":[]});

    // test the non-cached path
    let url = ["http://", &SERVER_IP, ":", &NO_CACHE_PORT, "/api/sibling"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);

    // test the cached path
    let url = ["http://", &SERVER_IP, ":", &CACHE_PORT, "/api/sibling"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);
}

#[test]
fn get_table_records_unsupported_type() {
    run_setup();

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_fields?columns=id,test_bit,test_varbit",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/test_fields?columns=id,test_bit,test_varbit",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_empty_where() {
    run_setup();

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_fields?columns=id&where=",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/test_fields?columns=id&where=",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_simple_where() {
    run_setup();
    let expected_response_body = json!([{"id": 46_327_143_679_919_107i64, "test_name": "a name"}]);

    // test the non-cached path
    let url = ["http://", &SERVER_IP, ":", &NO_CACHE_PORT, "/api/test_fields?columns=id,test_name&where=id%20%3D%2046327143679919107%20AND%20test_name%20%3D%20%27a%20name%27"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);

    // test the cached path
    let url = ["http://", &SERVER_IP, ":", &CACHE_PORT, "/api/test_fields?columns=id,test_name&where=id%20%3D%2046327143679919107%20AND%20test_name%20%3D%20%27a%20name%27"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);
}

#[test]
fn get_table_record_aggregates() {
    run_setup();
    let expected_response_body = json!([{"id": 46_327_143_679_919_107i64, "count": 1}]);

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_fields?columns=id,COUNT(id)&group_by=id&order_by=COUNT(id)",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/test_fields?columns=id,COUNT(id)&group_by=id&order_by=COUNT(id)",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_record_alias() {
    run_setup();

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_fields?columns=COUNT(id) AS counted_ids&group_by=id&order_by=counted_ids",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{"counted_ids": 1}]));
    assert_eq!(res.status(), StatusCode::OK);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/test_fields?columns=COUNT(id) AS counted_ids&group_by=id&order_by=counted_ids",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{"counted_ids": 1}]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_wildcards() {
    run_setup();

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/child?columns=*",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/child?columns=*",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_foreign_keys() {
    run_setup();
    let expected_response_body = json!([{
        "id": 1,
        "name": "Ned",
        "company_id.name": "Stark Corporation",
    }]);

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/adult?columns=id,name,company_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/adult?columns=id,name,company_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_body, expected_response_body);
}

#[test]
fn get_table_records_foreign_keys_dot_misuse() {
    run_setup();

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/adult?columns=id,company_id.",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/adult?columns=id,company_id.",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_foreign_keys_nested() {
    run_setup();
    let expected_response_body = json!([{
        "id": 1000,
        "name": "Robb",
        "parent_id.name": "Ned",
        "parent_id.company_id.name": "Stark Corporation",
    }]);

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_foreign_keys_nested_aliases() {
    run_setup();
    let expected_response_body = json!([{
        "id": 1000,
        "name": "Robb",
        "parent_name": "Ned",
        "parent_company_name": "Stark Corporation",
    }]);

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP, ":",
        &NO_CACHE_PORT,
        "/api/child?columns=id,name,parent_id.name as parent_name,parent_id.company_id.name as parent_company_name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP, ":",
        &CACHE_PORT,
        "/api/child?columns=id,name,parent_id.name as parent_name,parent_id.company_id.name as parent_company_name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_foreign_keys_multiple_tables() {
    run_setup();
    let expected_response_body = json!([{
        "id": 1000,
        "name": "Robb",
        "parent_id.name": "Ned",
        "parent_id.company_id.name": "Stark Corporation",
        "school_id.name": "Winterfell Tower",
    }]);

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name,school_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name,school_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, expected_response_body);
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_foreign_key_wildcards() {
    run_setup();

    // test the non-cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/adult?columns=id,name,company_id.*",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // test the cached path
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &CACHE_PORT,
        "/api/adult?columns=id,name,company_id.*",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn post_table_record() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 1}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 1 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 2}, {"id": 3}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 2 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_no_body() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert",
    ]
    .join("");
    let res = Client::new().request(Method::POST, &url).send().unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn post_table_records_returning_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert?returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 4, "name": "A"}, {"id": 5, "name": "b"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{ "id": 4, "name": "A" }, { "id": 5, "name": "b" }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_on_conflict_do_nothing() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 14, "name": "A"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 1 }));
    assert_eq!(res.status(), StatusCode::OK);

    // now attempt to send same request but with different name
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert?conflict_action=nothing&conflict_target=id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 14, "name": "B"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 0 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_on_conflict_do_nothing_returning_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert?returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 10, "name": "A"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "id": 10, "name": "A" }]));
    assert_eq!(res.status(), StatusCode::OK);

    // now attempt to send same request but with different name
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert?returning_columns=id, name&conflict_action=nothing&conflict_target=id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 10, "name": "B"}, {"id": 11, "name": "C"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "id": 11, "name": "C" }]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_on_conflict_update() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert?returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 12, "name": "A"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "id": 12, "name": "A" }]));
    assert_eq!(res.status(), StatusCode::OK);

    // now attempt to send same request but with different name
    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/test_insert?returning_columns=id, name&conflict_action=update&conflict_target=id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 12, "name": "B"}, {"id": 13, "name": "C"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{"id": 12, "name": "B"}, {"id": 13, "name": "C"}])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_simple() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=id%3D5",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"team_id": 5}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 1 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_simple_returning_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=id%3D5&returning_columns=team_id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"team_id": 5}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "team_id": 5 }]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_string_value() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=name%3D'Russell Westbrook'&returning_columns=name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"name": "'Chris Paul'"}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "name": "Chris Paul" }]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_fk_in_where() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=team_id.name%3D'LA Clippers'",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"team_id": 3}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 2 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_fk_in_body() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=id%3D1&returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"name": "team_id.name"}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{ "id": 1, "name": "Golden State Warriors" }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_nested_fk_in_returning_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=id%3D2&returning_columns=id, name, team_id.name, team_id.coach_id.name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"name": "team_id.coach_id.name"}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{ "id": 2, "name": "Steve Kerr", "team_id.name": "Golden State Warriors", "team_id.coach_id.name": "Steve Kerr" }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn put_table_records_nested_fk_in_returning_column_aliases() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/player?where=id%3D2&returning_columns=id, team_id.name as team_name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::PUT, &url)
        .json(&json!({"name": "team_id.coach_id.name"}))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{ "id": 2, "team_name": "Golden State Warriors" }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn delete_table_records_no_confirm() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/delete_simple",
    ]
    .join("");
    let res = Client::new().request(Method::DELETE, &url).send().unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn delete_table_records_simple() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/delete_simple?confirm_delete",
    ]
    .join("");
    let mut res = Client::new().request(Method::DELETE, &url).send().unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 3 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn delete_table_records_conditions() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/delete_a?confirm_delete&where=id%3D1",
    ]
    .join("");
    let mut res = Client::new().request(Method::DELETE, &url).send().unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 1 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn delete_table_records_conditions_return_fk_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/delete_a?confirm_delete&where=id%3D3&returning_columns=b_id.id",
    ]
    .join("");
    let mut res = Client::new().request(Method::DELETE, &url).send().unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "b_id.id": 2 }]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn delete_table_records_return_fk_column_alias() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP,
        ":",
        &NO_CACHE_PORT,
        "/api/delete_a?confirm_delete&where=id%3D5&returning_columns=b_id.id as b_id",
    ]
    .join("");
    let mut res = Client::new().request(Method::DELETE, &url).send().unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "b_id": 4 }]));
    assert_eq!(res.status(), StatusCode::OK);
}
