# Create the database that we'll use to populate data and watch the effect in the binlog
CREATE DATABASE connector_test;

# Create and populate our products using a single insert with many rows
CREATE TABLE connector_test.products (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description VARCHAR(512),
  weight FLOAT
);
ALTER TABLE connector_test.products AUTO_INCREMENT = 101;

# Create and populate the products on hand using multiple inserts
CREATE TABLE connector_test.products_on_hand (
  product_id INTEGER NOT NULL PRIMARY KEY,
  quantity INTEGER NOT NULL,
  FOREIGN KEY (product_id) REFERENCES products(id)
);

# Create some customers ...
CREATE TABLE connector_test.customers (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT=1001;