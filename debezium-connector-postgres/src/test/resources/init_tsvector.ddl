-- Separate file because tsvector is tested since PostgreSQL 16
CREATE SCHEMA IF NOT EXISTS public;
DROP SCHEMA IF EXISTS tsvector CASCADE;
CREATE SCHEMA tsvector;
