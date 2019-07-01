-- noinspection SqlNoDataSourceInspectionForFile

-- Separate file because PostGIS populates the spatial_ref_sys table on load
CREATE SCHEMA IF NOT EXISTS public;
DROP SCHEMA IF EXISTS postgis CASCADE;
CREATE SCHEMA postgis;
CREATE EXTENSION IF NOT EXISTS postgis SCHEMA postgis;

-- When extensions are installed on Amazon RDS, they'll be installed and owned by rdsadmin.
-- Unfortunately when extensions are owned by this super user, certain operations won't be allowed by our connector user.
-- By changing the ownership of the special_ref_sys table, tests will be able to perform table locks during snapshot.
ALTER TABLE postgis.spatial_ref_sys OWNER to SESSION_USER;
