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
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

-- For testing fields

DROP TABLE IF EXISTS public.test_fields;
CREATE TABLE IF NOT EXISTS public.test_fields (
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

INSERT INTO public.test_fields (
  id,
  test_bigint,
  test_bigserial,
  test_bit,
  test_bool,
  test_bytea,
  test_char,
  test_citext,
  test_date,
  test_float8,
  test_hstore,
  test_int,
  test_json,
  test_jsonb,
  test_macaddr,
  test_name,
  test_oid,
  test_real,
  test_serial,
  test_smallint,
  test_smallserial,
  test_text,
  test_time,
  test_timestamp,
  test_timestamptz,
  test_uuid,
  test_varbit,
  test_varchar,
  test_f64,
  test_decimal,
  test_numeric
) VALUES (
  46327143679919107,
  -9001,
  9001,
  1::BIT,
  't',
  '\\x5c313334',
  'a',
  'test citext',
  '2018-12-31',
  123.456000000000003,
  '"a"=>"1.0", "b"=>"2.4"',
  -123,
  '{"a": 123,"b": "cde"}',
  '{"a": 456,"c": "def"}',
  '08:00:2b:01:02:03',
  'a name',
  null,
  -1.39999998,
  2,
  -50,
  1,
  'some text',
  '04:05:00',
  '2004-10-19 10:23:54',
  '2004-10-19 03:23:54-05',
  'cf53dec3-18b5-4342-aedc-d7d881316bed',
  '101'::BIT VARYING,
  'a varchar',
  1.31479999999999997,
  100.01,
  100.02
);

-- For testing foreign keys
DROP TABLE IF EXISTS public.child;
DROP TABLE IF EXISTS public.adult;
DROP TABLE IF EXISTS public.school;
DROP TABLE IF EXISTS public.company;

CREATE TABLE public.company (
  id BIGINT CONSTRAINT company_id_key PRIMARY KEY,
  name TEXT
);

CREATE TABLE public.school (
  id BIGINT CONSTRAINT school_id_key PRIMARY KEY,
  name TEXT
);

CREATE TABLE public.adult (
  id BIGINT CONSTRAINT adult_id_key PRIMARY KEY,
  company_id BIGINT,
  name TEXT
);
ALTER TABLE public.adult ADD CONSTRAINT adult_company_id FOREIGN KEY (company_id) REFERENCES public.company(id);

CREATE TABLE public.child (
  id BIGINT CONSTRAINT child_id_key PRIMARY KEY,
  parent_id BIGINT,
  school_id BIGINT,
  name TEXT
);
ALTER TABLE public.child ADD CONSTRAINT child_parent_id FOREIGN KEY (parent_id) REFERENCES public.adult(id);
ALTER TABLE public.child ADD CONSTRAINT child_school_id FOREIGN KEY (school_id) REFERENCES public.school(id);

INSERT INTO public.company (id, name) VALUES (100, 'Stark Corporation');
INSERT INTO public.school (id, name) VALUES (10, 'Winterfell Tower');
INSERT INTO public.adult (id, company_id, name) VALUES (1, 100, 'Ned');
INSERT INTO public.child (id, name, parent_id, school_id) VALUES (1000, 'Robb', 1, 10);

-- For testing INSERTs

DROP TABLE IF EXISTS public.test_insert;
DROP TABLE IF EXISTS public.test_batch_insert;

CREATE TABLE public.test_insert (
  id BIGINT CONSTRAINT test_insert_id_key PRIMARY KEY,
  name TEXT
);

CREATE TABLE public.test_batch_insert (
  id BIGINT CONSTRAINT test_batch_insert_id_key PRIMARY KEY
);
