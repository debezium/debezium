-- noinspection SqlNoDataSourceInspectionForFile

-- Separate file because pgvector is tested since PostgreSQL 15
CREATE SCHEMA IF NOT EXISTS public;
DROP SCHEMA IF EXISTS pgvector CASCADE;
CREATE SCHEMA pgvector;
CREATE EXTENSION IF NOT EXISTS vector SCHEMA pgvector;
