-- With our without binlog
flush no_write_to_binlog hosts;
flush local hosts;
flush hosts, status;

-- Table flushing
flush tables;
flush local tables Foo;
flush tables Foo, Bar;
flush tables Foo, Bar for export;
flush tables Foo, Bar with read lock;

-- 'FLUSH TABLE' is an alias for 'FLUSH TABLES' (https://dev.mysql.com/doc/refman/8.0/en/flush.html)
flush table;
flush local table Foo;
flush TABLE Foo, Bar;
flush table Foo, Bar for export;
flush table Foo, Bar with read lock;

-- Azure Database for MySQL Single Server instance. This type of database server is being decommissioned on Sept 16 2024 and is succeeded by their "Flexible Server" offering.
FLUSH FIREWALL_RULES;



