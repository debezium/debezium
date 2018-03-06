-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE public.postgis_table (pk SERIAL, p postgis.GEOMETRY(POINT,3187), ml postgis.GEOGRAPHY(MULTILINESTRING), PRIMARY KEY(pk));
CREATE TABLE public.postgis_array_table (pk SERIAL, ga postgis.GEOMETRY[], gann postgis.GEOMETRY[] NOT NULL, PRIMARY KEY(pk));
CREATE TABLE public.dummy_table (pk SERIAL, PRIMARY KEY(pk));
