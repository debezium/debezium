-- noinspection SqlNoDataSourceInspectionForFile

-- Generate a number of tables to cover as many of the PG types as possible
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- load contrib extensions for testing non-builtin types
CREATE EXTENSION IF NOT EXISTS ltree SCHEMA public;
CREATE EXTENSION IF NOT EXISTS isn SCHEMA public;
CREATE EXTENSION IF NOT EXISTS citext SCHEMA public;
CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public;
