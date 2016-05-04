/* contrib/pipeline_kinesis/pipeline_kinesis--0.9.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_kinesis" to load this file. \quit

CREATE TABLE pipeline_kinesis_endpoints (
	name text PRIMARY KEY,
	region text NOT NULL,
	credfile text NOT NULL,
	url text
) WITH OIDS;

-- Consumers added with kinesis_consume_begin
CREATE TABLE pipeline_kinesis_consumers (
  endpoint    text    NOT NULL,
  relation    text    NOT NULL,
  stream 	  text    NOT NULL,
  batchsize   integer NOT NULL,
  parallelism integer NOT NULL
) WITH OIDS;

CREATE TABLE pipeline_kinesis_seqnums (
	consumer_id oid NOT NULL,
	shard integer NOT NULL,
	seqnum text NOT NULL
);

CREATE FUNCTION kinesis_add_endpoint (
  name text,
  region text,
  credfile text,
  url text DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_add_endpoint'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kinesis_remove_endpoint (
  name text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_remove_endpoint'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION kinesis_consume_begin_sr (
  endpoint     text,
  stream 	   text,
  relation     text,
  batchsize    integer DEFAULT 1000,
  parallelism  integer DEFAULT 1,
  start_seq text DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_consume_begin_sr'
LANGUAGE C IMMUTABLE;

--CREATE FUNCTION kinesis_consume_end_sr (
--  endpoint   text,
--  stream 	text,
--  relation     text
--)
--RETURNS text
--AS 'MODULE_PATHNAME', 'kinesis_consume_end_sr'
--LANGUAGE C IMMUTABLE;
