-- Generate a number of tables to cover as many of the PG types as possible
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
CREATE TABLE numeric_table (pk SERIAL, si SMALLINT, i INTEGER, bi BIGINT, d DECIMAL(3,2), n NUMERIC(5,3), r REAL, db DOUBLE PRECISION, ss SMALLSERIAL, bs BIGSERIAL, b BOOLEAN, PRIMARY KEY(pk));
CREATE TABLE string_table (pk SERIAL, vc VARCHAR(2), vcv CHARACTER VARYING(2), ch CHARACTER(4), c CHAR(3), t TEXT, PRIMARY KEY(pk));
CREATE TABLE cash_table (pk SERIAL, csh MONEY, PRIMARY KEY(pk));
CREATE TABLE bitbin_table (pk SERIAL, ba BYTEA, bol BIT(1), bs BIT(2), bv BIT VARYING(2) , PRIMARY KEY(pk));
CREATE TABLE time_table (pk SERIAL, ts TIMESTAMP, tz TIMESTAMPTZ, date DATE, ti TIME, ttz TIME WITH TIME ZONE, it INTERVAL, PRIMARY KEY(pk));
CREATE TABLE text_table (pk SERIAL, j JSON, jb JSONB, x XML, u Uuid, PRIMARY KEY(pk));
CREATE TABLE geom_table (pk SERIAL, p POINT, PRIMARY KEY(pk));
CREATE TABLE tstzrange_table (pk serial, unbounded_exclusive_range tstzrange, bounded_inclusive_range tstzrange, PRIMARY KEY(pk));
