#begin
OPTIMIZE TABLE t1;
OPTIMIZE TABLE t1, t2;
#NB: OPTIMIZE TABLES t1;
#NB: OPTIMIZE TABLES t1, t2;
optimize local table t1;
#end
