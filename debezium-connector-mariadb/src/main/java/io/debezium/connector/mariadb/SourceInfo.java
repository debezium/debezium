/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.binlog.BinlogSourceInfo;

/**
 * Information about the source, which includes the position in the binary transaction log.<p></p>
 *
 * The {@link MariaDbPartition#getSourcePartition() source partition} information describes the database whose
 * log is being consumed. Typically, the database is identified by the host address and port number of the
 * MariaDB server and the name of the database. Here's an example JSON representation:
 *
 * <pre>
 *     {
 *         "server": "production-server"
 *     }
 * </pre>
 *
 * The offset includes the {@link #binlogFileName() binlog filename}, the {@link #binlogPosition() position of the first event}
 * in the transaction log, the {@link MariaDbOffsetContext#eventsToSkipUponRestart() number of events to skip on restart},
 * and the {@link MariaDbOffsetContext#rowsToSkipUponRestart() number of rows to skip}. An example:
 *
 * <pre>
 *     {
 *         "server_id": 112233,
 *         "ts_ms": 123456789,
 *         "gtid": "0-1-3",
 *         "file": "binlog.000003",
 *         "pos": 990,
 *         "event": 0,
 *         "row": 0,
 *         "snapshot": true
 *     }
 * </pre>
 *
 * The "{@code snapshot}" field only appears in offsets produced during the snapshot phase. The "{@code ts_ms}" field
 * contains the <em>milliseconds</em> since Unix epoch (since Jan 1 1970) of the MariaDB event.<p></p>
 *
 * Each change event {@link io.debezium.data.Envelope} also contains a {@link #struct() source} struct that contains
 * the MariaDB information about that specific event, including a mixture of fields from the binary log filename and
 * position where the event can be found, GTID details, etc. Like with the offset, the "{@code snapshot}" field will
 * only appear for events produced during the snapshot phase.<p></p>
 *
 * Here's a JSON example of the source metadata for an event:
 * <pre>
 *     {
 *         "name": "production-server",
 *         "server_id": 112233,
 *         "ts_ms": 123456789,
 *         "gtid": "0-1-3",
 *         "file": "binlog.000003",
 *         "pos": 1081,
 *         "row": 0,
 *         "snapshot": true,
 *         "thread": 1,
 *         "db": "inventory",
 *         "table": "products"
 *     }
 * </pre>
 *
 * @author Chris Cranford
 */
@NotThreadSafe
public class SourceInfo extends BinlogSourceInfo {
    public SourceInfo(MariaDbConnectorConfig connectorConfig) {
        super(connectorConfig);
    }
}
