#begin
KILL CONNECTION 12345;
KILL QUERY 12345;
KILL CONNECTION @conn_variable;
KILL QUERY @query_variable;
KILL CONNECTION @@global_variable;
KILL QUERY @@global_variable;
#end
