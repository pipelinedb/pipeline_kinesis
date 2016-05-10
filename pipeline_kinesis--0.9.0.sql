-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipeline_kinesis" to load this file. \quit

CREATE TABLE pipeline_kinesis.endpoints (
  name text PRIMARY KEY,
  region text NOT NULL,
  credfile text,
  url text
);

-- Consumers added with pipeline_kinesis.consume_begin
CREATE TABLE pipeline_kinesis.consumers (
  id serial PRIMARY KEY,
  endpoint text REFERENCES pipeline_kinesis.endpoints(name),
  stream text NOT NULL,
  relation text NOT NULL,
  format text    NOT NULL,
  delimiter text,
  quote text,
  escape text,
  batchsize integer NOT NULL,
  parallelism integer NOT NULL,
  UNIQUE(endpoint, stream, relation)
);

CREATE TABLE pipeline_kinesis.seqnums (
  consumer_id int REFERENCES pipeline_kinesis.consumers(id),
  shard_id text NOT NULL,
  seqnum text NOT NULL,
  PRIMARY KEY(consumer_id, shard_id)
);

CREATE FUNCTION pipeline_kinesis.add_endpoint (
  name text,
  region text,
  credfile text,
  url text DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_add_endpoint'
LANGUAGE C VOLATILE;

CREATE FUNCTION pipeline_kinesis.remove_endpoint (
  name text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_remove_endpoint'
LANGUAGE C VOLATILE;

CREATE FUNCTION pipeline_kinesis.consume_begin (
  endpoint text,
  stream text,
  relation text,
  format text DEFAULT 'text',
  delimiter text DEFAULT E'\t',
  quote text DEFAULT NULL,
  escape text DEFAULT NULL,
  batchsize integer DEFAULT 1000,
  parallelism integer DEFAULT 1,
  start_seq integer DEFAULT NULL
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_consume_begin_sr'
LANGUAGE C VOLATILE;

CREATE FUNCTION pipeline_kinesis.consume_end (
  endpoint text,
  stream text,
  relation text
)
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_consume_end_sr'
LANGUAGE C VOLATILE;

CREATE FUNCTION pipeline_kinesis.consume_begin_all()
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_consume_begin_all'
LANGUAGE C VOLATILE;

CREATE FUNCTION pipeline_kinesis.consume_end_all()
RETURNS text
AS 'MODULE_PATHNAME', 'kinesis_consume_end_all'
LANGUAGE C VOLATILE;
