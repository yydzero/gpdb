/* contrib/masteronly/masteronly--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION masteronly" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS masteronly;

SET search_path = public,masteronly;

CREATE OR REPLACE FUNCTION  masteronly.set_master_only_table(tbl REGCLASS)
RETURNS NAME
AS 'MODULE_PATHNAME', 'set_master_only_table'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION  masteronly.enable_singleton_table()
RETURNS bool
AS 'MODULE_PATHNAME', 'enable_singleton_table'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION  masteronly.disable_singleton_table()
RETURNS bool
AS 'MODULE_PATHNAME', 'enable_singleton_table'
LANGUAGE C IMMUTABLE STRICT;
