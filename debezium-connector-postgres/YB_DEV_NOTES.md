# YugabyteDB Development Instructions

## Compiling code

Since the smart driver changes require us to build the debezium core as well, build can be completed using:

```bash
./mvnw clean install -Dquick
```

## Running tests

1. Compile PG connector code from the root directory with the above command.
2. Start YugabyteDB instance using `yugabyted`:
   ```bash
   ./bin/yugabyted start --ui=false --advertise_address=127.0.0.1 --master_flags="yb_enable_cdc_consistent_snapshot_streams=true,allowed_preview_flags_csv={yb_enable_cdc_consistent_snapshot_streams,ysql_yb_enable_replication_commands},ysql_yb_enable_replication_commands=true,ysql_TEST_enable_replication_slot_consumption=true" --tserver_flags="allowed_preview_flags_csv={yb_enable_cdc_consistent_snapshot_streams,ysql_yb_enable_replication_commands},ysql_yb_enable_replication_commands=true,yb_enable_cdc_consistent_snapshot_streams=true,ysql_TEST_enable_replication_slot_consumption=true,ysql_cdc_active_replication_slot_window_ms=0,ysql_sequence_cache_method=server"
   ```
3. Run tests 