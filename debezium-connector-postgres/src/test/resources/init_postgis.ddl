-- noinspection SqlNoDataSourceInspectionForFile

-- Separate file because PostGIS populates the spatial_ref_sys table on load
CREATE SCHEMA IF NOT EXISTS public;
DROP SCHEMA IF EXISTS postgis CASCADE;
CREATE SCHEMA postgis;
CREATE EXTENSION IF NOT EXISTS postgis SCHEMA postgis;

