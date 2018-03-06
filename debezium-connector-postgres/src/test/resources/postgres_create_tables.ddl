-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE numeric_table (pk SERIAL, si SMALLINT, i INTEGER, bi BIGINT,
    r REAL, db DOUBLE PRECISION,
    r_int REAL, db_int DOUBLE PRECISION,
    r_nan REAL, db_nan DOUBLE PRECISION,
    r_pinf REAL, db_pinf DOUBLE PRECISION,
    r_ninf REAL, db_ninf DOUBLE PRECISION,
    ss SMALLSERIAL, bs BIGSERIAL, b BOOLEAN, PRIMARY KEY(pk));
-- no suffix -fixed scale, zs - zero scale, vs - variable scale
CREATE TABLE numeric_decimal_table (pk SERIAL,
	d DECIMAL(3,2), dzs DECIMAL(4), dvs DECIMAL, d_nn DECIMAL(3,2) NOT NULL, n NUMERIC(6,4), nzs NUMERIC(4), nvs NUMERIC,
	d_int DECIMAL(3,2), dzs_int DECIMAL(4), dvs_int DECIMAL, n_int NUMERIC(6,4), nzs_int NUMERIC(4), nvs_int NUMERIC,
	d_nan DECIMAL(3,2), dzs_nan DECIMAL(4), dvs_nan DECIMAL, n_nan NUMERIC(6,4), nzs_nan NUMERIC(4), nvs_nan NUMERIC,
	PRIMARY KEY(pk));
CREATE TABLE string_table (pk SERIAL, vc VARCHAR(2), vcv CHARACTER VARYING(2), ch CHARACTER(4), c CHAR(3), t TEXT, b BYTEA, bnn BYTEA NOT NULL, PRIMARY KEY(pk));
CREATE TABLE cash_table (pk SERIAL, csh MONEY, PRIMARY KEY(pk));
CREATE TABLE bitbin_table (pk SERIAL, ba BYTEA, bol BIT(1), bs BIT(2), bv BIT VARYING(2) , PRIMARY KEY(pk));
CREATE TABLE time_table (pk SERIAL, ts TIMESTAMP, tz TIMESTAMPTZ, date DATE, ti TIME, ttz TIME WITH TIME ZONE, it INTERVAL, tsp TIMESTAMP (0) WITH TIME ZONE, PRIMARY KEY(pk));
CREATE TABLE text_table (pk SERIAL, j JSON, jb JSONB, x XML, u Uuid, PRIMARY KEY(pk));
CREATE TABLE geom_table (pk SERIAL, p POINT, PRIMARY KEY(pk));
CREATE TABLE tstzrange_table (pk serial, unbounded_exclusive_range tstzrange, bounded_inclusive_range tstzrange, PRIMARY KEY(pk));
CREATE TABLE array_table (pk SERIAL, int_array INT[], bigint_array BIGINT[], text_array TEXT[], char_array CHAR(10)[], varchar_array VARCHAR(10)[], date_array DATE[], numeric_array NUMERIC(10, 2)[], varnumeric_array NUMERIC[3], PRIMARY KEY(pk));
CREATE TABLE array_table_with_nulls (pk SERIAL, int_array INT[], bigint_array BIGINT[], text_array TEXT[], char_array CHAR(10)[], varchar_array VARCHAR(10)[], date_array DATE[], numeric_array NUMERIC(10, 2)[], varnumeric_array NUMERIC[3], PRIMARY KEY(pk));
CREATE TABLE custom_table (pk serial, lt ltree, i isbn, n TEXT, PRIMARY KEY(pk));

DROP SCHEMA IF EXISTS "Quoted_"" . Schema" CASCADE;
CREATE SCHEMA "Quoted_"" . Schema";
-- GRANT ALL ON ALL TABLES IN SCHEMA "Quoted_Schema" TO postgres;
CREATE TABLE "Quoted_"" . Schema"."Quoted_"" . Table" (pk SERIAL, "Quoted_"" . Text_Column" TEXT, PRIMARY KEY(pk));
