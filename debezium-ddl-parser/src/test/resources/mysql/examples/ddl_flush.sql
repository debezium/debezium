#begin
flush hosts, status;
#end
#begin
-- Table flushing
flush tables;
flush local tables Foo;
flush tables Foo, Bar;
flush tables Foo, Bar for export;
flush tables Foo, Bar with read lock;
#end
#begin
-- 'FLUSH TABLE' is an alias for 'FLUSH TABLES' (https://dev.mysql.com/doc/refman/8.0/en/flush.html)
flush table;
flush local table Foo;
flush TABLE Foo, Bar;
flush table Foo, Bar for export;
#end