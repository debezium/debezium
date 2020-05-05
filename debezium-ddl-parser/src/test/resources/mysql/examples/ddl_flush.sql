-- With our without binlog
flush no_write_to_binlog hosts;
flush local hosts;
flush hosts, status;

-- Table flushing
flush local tables Foo;
flush tables Foo, Bar;
flush tables Foo, Bar for export;
flush tables Foo, Bar with read lock;

-- 'FLUSH TABLE' is an alias for 'FLUSH TABLES' (https://dev.mysql.com/doc/refman/8.0/en/flush.html)
flush local table Foo;
flush TABLE Foo, Bar;
flush table Foo, Bar for export;
flush table Foo, Bar with read lock;



