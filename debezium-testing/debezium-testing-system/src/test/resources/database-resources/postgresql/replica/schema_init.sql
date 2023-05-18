SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

CREATE SCHEMA inventory;

ALTER SCHEMA inventory OWNER TO ${database.postgresql.username};

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA inventory;


COMMENT ON EXTENSION postgis IS 'PostGIS geometry and geography spatial types and functions';

SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE TABLE inventory.customers (
    id integer NOT NULL,
    first_name character varying(255) NOT NULL,
    last_name character varying(255) NOT NULL,
    email character varying(255) NOT NULL
);

ALTER TABLE ONLY inventory.customers REPLICA IDENTITY FULL;


ALTER TABLE inventory.customers OWNER TO ${database.postgresql.dbname};

CREATE SEQUENCE inventory.customers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE inventory.customers_id_seq OWNER TO ${database.postgresql.dbname};


ALTER SEQUENCE inventory.customers_id_seq OWNED BY inventory.customers.id;

CREATE TABLE inventory.geom (
    id integer NOT NULL,
    g inventory.geometry NOT NULL,
    h inventory.geometry
);


ALTER TABLE inventory.geom OWNER TO ${database.postgresql.dbname};

CREATE SEQUENCE inventory.geom_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE inventory.geom_id_seq OWNER TO ${database.postgresql.dbname};

ALTER SEQUENCE inventory.geom_id_seq OWNED BY inventory.geom.id;

CREATE TABLE inventory.orders (
    id integer NOT NULL,
    order_date date NOT NULL,
    purchaser integer NOT NULL,
    quantity integer NOT NULL,
    product_id integer NOT NULL
);

ALTER TABLE ONLY inventory.orders REPLICA IDENTITY FULL;

ALTER TABLE inventory.orders OWNER TO ${database.postgresql.dbname};

CREATE SEQUENCE inventory.orders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE inventory.orders_id_seq OWNER TO ${database.postgresql.dbname};

ALTER SEQUENCE inventory.orders_id_seq OWNED BY inventory.orders.id;

CREATE TABLE inventory.products (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    description character varying(512),
    weight double precision
);

ALTER TABLE ONLY inventory.products REPLICA IDENTITY FULL;

ALTER TABLE inventory.products OWNER TO ${database.postgresql.dbname};

CREATE SEQUENCE inventory.products_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE inventory.products_id_seq OWNER TO ${database.postgresql.dbname};

ALTER SEQUENCE inventory.products_id_seq OWNED BY inventory.products.id;

CREATE TABLE inventory.products_on_hand (
    product_id integer NOT NULL,
    quantity integer NOT NULL
);

ALTER TABLE ONLY inventory.products_on_hand REPLICA IDENTITY FULL;

ALTER TABLE inventory.products_on_hand OWNER TO ${database.postgresql.dbname};

ALTER TABLE ONLY inventory.customers ALTER COLUMN id SET DEFAULT nextval('inventory.customers_id_seq'::regclass);

ALTER TABLE ONLY inventory.geom ALTER COLUMN id SET DEFAULT nextval('inventory.geom_id_seq'::regclass);

ALTER TABLE ONLY inventory.orders ALTER COLUMN id SET DEFAULT nextval('inventory.orders_id_seq'::regclass);

ALTER TABLE ONLY inventory.products ALTER COLUMN id SET DEFAULT nextval('inventory.products_id_seq'::regclass);

ALTER TABLE ONLY inventory.customers
    ADD CONSTRAINT customers_email_key UNIQUE (email);

ALTER TABLE ONLY inventory.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY inventory.geom
    ADD CONSTRAINT geom_pkey PRIMARY KEY (id);

ALTER TABLE ONLY inventory.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);

ALTER TABLE ONLY inventory.products_on_hand
    ADD CONSTRAINT products_on_hand_pkey PRIMARY KEY (product_id);

ALTER TABLE ONLY inventory.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (id);

ALTER TABLE ONLY inventory.orders
    ADD CONSTRAINT orders_product_id_fkey FOREIGN KEY (product_id) REFERENCES inventory.products(id);

ALTER TABLE ONLY inventory.orders
    ADD CONSTRAINT orders_purchaser_fkey FOREIGN KEY (purchaser) REFERENCES inventory.customers(id);

ALTER TABLE ONLY inventory.products_on_hand
    ADD CONSTRAINT products_on_hand_product_id_fkey FOREIGN KEY (product_id) REFERENCES inventory.products(id);

CREATE SUBSCRIPTION dbz_subscription CONNECTION 'host=postgresql-primary port=${database.postgresql.port} user=${database.postgresql.username} password=${database.postgresql.password} dbname=${database.postgresql.dbname}' PUBLICATION dbz_publication;
