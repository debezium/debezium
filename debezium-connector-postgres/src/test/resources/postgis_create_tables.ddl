-- noinspection SqlNoDataSourceInspectionForFile

-- Separate file because PostGIS populates the spatial_ref_sys table on load
CREATE SCHEMA IF NOT EXISTS public;
DROP SCHEMA IF EXISTS postgis CASCADE;
CREATE SCHEMA postgis;
CREATE EXTENSION IF NOT EXISTS postgis SCHEMA postgis;


CREATE TABLE public.postgis_table (pk SERIAL, p postgis.GEOMETRY(POINT,3187), ml postgis.GEOGRAPHY(MULTILINESTRING), PRIMARY KEY(pk));
CREATE TABLE public.postgis_array_table (pk SERIAL, ga postgis.GEOMETRY[], PRIMARY KEY(pk));
CREATE TABLE public.dummy_table (pk SERIAL, PRIMARY KEY(pk));
