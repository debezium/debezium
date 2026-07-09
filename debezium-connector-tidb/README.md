# Debezium Connector for TiDB

This module is the first phase of TiDB support in Debezium, as discussed in
[DBZ-6269](https://issues.redhat.com/browse/DBZ-6269) /
[debezium/dbz#789](https://github.com/debezium/dbz/issues/789).

## Architecture (phase 1)

TiDB does not expose a MySQL binlog and the only consumer of TiKV's raw `EventFeed` gRPC API is
TiCDC itself, which resolves raw key-values into rows using its schema snapshots. Rather than
re-implementing that translation layer, this connector consumes the Kafka output of a TiCDC
changefeed running in its native Debezium protocol mode (TiDB v8.0+):

```
cdc cli changefeed create \
  --sink-uri="kafka://<broker>:9092/<topic>?protocol=debezium"
```

```
TiDB/TiKV ──> TiCDC (changefeed, protocol=debezium) ──> Kafka topic(s)
                                                            │
                                                            ▼
                                          Debezium TiDB connector (this module)
                                                            │
                                                            ▼
                                       Kafka Connect topics (standard Debezium envelope)
```

On top of the raw TiCDC stream, the connector provides what a native Debezium connector is
expected to provide:

* **Connector lifecycle and offsets** — progress is stored in the Kafka Connect offset storage
  as the TiCDC topic/partition/offset positions plus the TSO `commit_ts` of the last processed
  transaction. Restarts resume exactly where the connector left off, no consumer group offsets
  are used.
* **A TiDB-specific `source` block** — TiCDC's Debezium output fills the MySQL-specific source
  fields with dummies (`server_id=0`, `gtid=null`, `file=""`, `pos=0`). The connector re-wraps
  the typed row images into its own envelope whose `source` struct carries the real position:
  `db`, `table`, `commit_ts` (TSO) and `cluster_id`.
* **Table filtering** — the standard `table.include.list`/`table.exclude.list` options are
  applied to the captured tables; TiDB system schemas are excluded by default.
* **Standard topic naming** — events are re-routed to `<topic.prefix>.<db>.<table>` topics using
  the configured topic naming strategy, independently of how the TiCDC changefeed partitions its
  output.

Schemas are learned from the typed TiCDC messages (the changefeed must produce JSON with inline
schemas, which is the default for `protocol=debezium`) and refreshed automatically when the
advertised row schema changes.

## Configuration

Minimal example:

```json
{
  "connector.class": "io.debezium.connector.tidb.TiDbConnector",
  "topic.prefix": "tidb_server",
  "ticdc.kafka.bootstrap.servers": "broker:9092",
  "ticdc.topics": "ticdc-changefeed-topic"
}
```

| Option | Default | Description |
| --- | --- | --- |
| `ticdc.kafka.bootstrap.servers` | — | Kafka cluster the TiCDC changefeed writes to. |
| `ticdc.topics` | — | Comma-separated list of topics written by the changefeed. |
| `ticdc.initial.offset` | `earliest` | Where to start reading when no offsets are stored (`earliest`/`latest`). |
| `ticdc.poll.timeout.ms` | `500` | Poll timeout of the internal consumer. |
| `ticdc.consumer.*` | — | Pass-through properties for the internal Kafka consumer (e.g. security settings). |
| `snapshot.mode` | `no_data` | `no_data` or `never`; data snapshots are not implemented yet (see roadmap). |

All common Debezium options (`table.include.list`, `topic.naming.strategy`, `tombstones.on.delete`,
SMTs, ...) apply as usual.

## Roadmap

Per the maintainer guidance in DBZ-6269:

1. **(this module)** Connector consuming TiCDC's Debezium-format Kafka output with Debezium
   lifecycle management.
2. **Managed snapshots** — initial and incremental snapshots through TiDB's MySQL-compatible SQL
   endpoint (`database.*` options), aligned with the relational connector framework.
3. **Direct TiKV streaming** — replace the TiCDC/Kafka dependency with a client of TiKV's
   `EventFeed` gRPC API; the rest of the connector (offsets keyed by `commit_ts`, envelope,
   snapshots) remains unchanged.

## Known limitations

* Data snapshots are skipped; use a TiCDC changefeed `start-ts` to backfill history.
* The TiCDC changefeed must emit JSON with inline schemas (`protocol=debezium` default).
* Decimal values arrive as `float64` from TiCDC's Debezium output; precise decimal encoding
  requires phase 2/3.
* DDL and watermark events (TiCDC v9+) are skipped; schema changes are picked up lazily from the
  data messages.
* Single task; parallelism is bounded by the TiCDC topic partitioning for now.
