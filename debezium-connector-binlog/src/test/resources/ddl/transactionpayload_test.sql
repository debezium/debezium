-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  transactionpayload_test
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our products using a single insert with many rows
CREATE TABLE products (
                          id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                          name VARCHAR(255) NOT NULL,
                          description VARCHAR(512),
                          weight FLOAT,
                          code BINARY(16)
);

-- Create some customers ...
CREATE TABLE customers (
                           id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                           first_name VARCHAR(255) NOT NULL,
                           last_name VARCHAR(255) NOT NULL,
                           email VARCHAR(255) NOT NULL UNIQUE KEY
) AUTO_INCREMENT=1001;

-- Create some very simple orders
CREATE TABLE orders (
                        order_number INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        order_date DATE NOT NULL,
                        purchaser INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        product_id INTEGER NOT NULL,
                        FOREIGN KEY order_customer (purchaser) REFERENCES customers(id),
                        FOREIGN KEY ordered_product (product_id) REFERENCES products(id)
) AUTO_INCREMENT = 10001;

CREATE DATABASE IF NOT EXISTS transactionpayload_test;
