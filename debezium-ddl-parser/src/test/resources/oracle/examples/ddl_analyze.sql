-- Analyze Table
analyze table schema.table123 estimate statistics;
analyze table schema.table123 estimate statistics for all columns;
analyze table schema.table123 delete statistics;
analyze table schema.table123 validate structure cascade;
analyze table schema.table123 validate ref update;
analyze table schema.table123 validate structure online;
analyze table schema.table123 list chained rows into chained_rows;
analyze table schema.table123 estimate statistics sample 5 percent;
analyze table schema.table123 compute statistics for table;
analyze table schema.table123 partition (partition_name) estimate statistics;
analyze table schema.table123 subpartition (partition_name) estimate statistics;
-- Analyze Index
analyze index schema.index123 estimate statistics;
analyze index schema.index123 estimate statistics for all columns;
analyze index schema.index123 delete statistics;
analyze index schema.index123 validate structure cascade;
analyze index schema.index123 validate ref update;
analyze index schema.index123 validate structure online;
analyze index schema.index123 list chained rows into chained_rows;
analyze index schema.index123 estimate statistics sample 5 percent;
analyze index schema.index123 compute statistics for table;
analyze index schema.index123 partition (partition_name) estimate statistics;
analyze index schema.index123 subpartition (subpartition_name) estimate statistics;
-- Analyze Cluster
analyze cluster schema.cluster123 estimate statistics;
analyze cluster schema.cluster123 estimate statistics for all columns;
analyze cluster schema.cluster123 delete statistics;
analyze cluster schema.cluster123 validate structure cascade;
analyze cluster schema.cluster123 validate ref update;
analyze cluster schema.cluster123 validate structure online;
analyze cluster schema.cluster123 list chained rows into chained_rows;
analyze cluster schema.cluster123 estimate statistics sample 5 percent;
analyze cluster schema.cluster123 compute statistics for table;