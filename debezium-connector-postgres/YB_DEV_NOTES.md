# YugabyteDB Development Instructions

## Compiling code

The following command will quickly build the postgres connector code with all the required dependencies and proto files:

```bash
./mvnw clean install -Dquick -pl debezium-connector-postgres -am 
```

## Running tests

1. Compile PG connector code from the root directory with the above command.
2. Start YugabyteDB instance using `yugabyted`:
   ```bash
   ./bin/yugabyted start --ui=false --advertise_address=127.0.0.1 --master_flags="yb_enable_cdc_consistent_snapshot_streams=true,allowed_preview_flags_csv={yb_enable_cdc_consistent_snapshot_streams,ysql_yb_enable_replication_commands},ysql_yb_enable_replication_commands=true,ysql_TEST_enable_replication_slot_consumption=true" --tserver_flags="allowed_preview_flags_csv={yb_enable_cdc_consistent_snapshot_streams,ysql_yb_enable_replication_commands},ysql_yb_enable_replication_commands=true,yb_enable_cdc_consistent_snapshot_streams=true,ysql_TEST_enable_replication_slot_consumption=true,ysql_cdc_active_replication_slot_window_ms=0,ysql_sequence_cache_method=server"
   ```
3. Run tests 