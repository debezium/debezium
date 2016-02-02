CREATE DATABASE emptydb;

-- Grant full privilege on the 'mysqluser' user connecting from any host
GRANT ALL PRIVILEGES ON emptydb.* TO 'mysqluser'@'%';
