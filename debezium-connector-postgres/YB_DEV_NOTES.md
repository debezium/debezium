# YugabyteDB Development Instructions

## Compiling code

Since the smart driver changes require us to build the debezium core as well, build can be completed using:

```bash
./mvnw clean install -Passembly -DskipITs -DskipTests -pl debezium-connector-postgres -am
```

## Running tests

1. Compile PG connector code from the root directory with the above command.
2. Start YugabyteDB instance using `yugabyted`:
   ```bash
   ./bin/yugabyted start --ui=false --advertise_address=127.0.0.1 --master_flags="ysql_cdc_active_replication_slot_window_ms=0" --tserver_flags="allowed_preview_flags_csv={cdcsdk_enable_dynamic_table_support},cdcsdk_enable_dynamic_table_support=true,cdcsdk_publication_list_refresh_interval_secs=3,ysql_cdc_active_replication_slot_window_ms=0,ysql_sequence_cache_method=server"
   ```
3. Run tests 