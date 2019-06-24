SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

DROP DATABASE IF EXISTS tests;
CREATE DATABASE tests WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8';

\connect tests

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;

CREATE TABLE public.test_fields (
    id bigint NOT NULL,
    test_bigint bigint,
    test_bigserial bigint NOT NULL,
    test_bit bit(1),
    test_bool boolean,
    test_bytea bytea,
    test_char character(1),
    test_citext public.citext,
    test_date date,
    test_float8 double precision,
    test_hstore public.hstore,
    test_int integer,
    test_json json,
    test_jsonb jsonb,
    test_macaddr macaddr,
    test_name name,
    test_oid oid,
    test_real real,
    test_serial integer NOT NULL,
    test_smallint smallint,
    test_smallserial smallint NOT NULL,
    test_text text,
    test_time time without time zone,
    test_timestamp timestamp without time zone,
    test_timestamptz timestamp with time zone,
    test_uuid uuid DEFAULT public.gen_random_uuid() NOT NULL,
    test_varbit bit varying,
    test_varchar character varying,
    test_f64 double precision,
    test_decimal numeric(5,2),
    test_numeric numeric(5,2)
);

ALTER TABLE ONLY public.test_fields
    ADD CONSTRAINT test_fields_pkey PRIMARY KEY (id);

COPY public.test_fields (id, test_bigint, test_bigserial, test_bit, test_bool, test_bytea, test_char, test_citext, test_date, test_float8, test_hstore, test_int, test_json, test_jsonb, test_macaddr, test_name, test_oid, test_real, test_serial, test_smallint, test_smallserial, test_text, test_time, test_timestamp, test_timestamptz, test_uuid, test_varbit, test_varchar, test_f64, test_decimal, test_numeric) FROM stdin;
46327143679919107	-9001	9001	1	t	\\x5c313334	a	test citext	2018-12-31	123.456000000000003	"a"=>"1.0", "b"=>"2.4"	-123	{"a": 123, "b": "cde"}	{"a": 456, "c": "def"}	08:00:2b:01:02:03	a name	\N	-1.39999998	2	-50	1	some text	04:05:00	2004-10-19 10:23:54	2004-10-19 03:23:54-05	cf53dec3-18b5-4342-aedc-d7d881316bed	101	a varchar	1.31479999999999997	100.01	100.02
\.

