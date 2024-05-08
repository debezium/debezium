/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.binlog.BinlogSourceInfo;
import io.debezium.data.Envelope;

/**
 * Information about the source of information, which includes the position in the source binary log we have previously processed.
 * <p>
 * The {@link MySqlPartition#getSourcePartition() source partition} information describes the database whose log is being consumed. Typically, the
 * database is identified by the host address port number of the MySQL server and the name of the database. Here's a JSON-like
 * representation of an example database:
 *
 * <pre>
 * {
 *     "server" : "production-server"
 * }
 * </pre>
 *
 * <p>
 * The offset includes the {@link #binlogFilename() binlog filename}, the {@link #binlogPosition() position of the first event} in the binlog, the
 * {@link MySqlOffsetContext#eventsToSkipUponRestart() number of events to skip}, and the
 * {@link MySqlOffsetContext#rowsToSkipUponRestart() number of rows to also skip}.
 * <p>
 * Here's a JSON-like representation of an example offset:
 *
 * <pre>
 * {
 *     "server_id": 112233,
 *     "ts_sec": 1465937,
 *     "gtid": "db58b0ae-2c10-11e6-b284-0242ac110002:199",
 *     "file": "mysql-bin.000003",
 *     "pos" = 990,
 *     "event" = 0,
 *     "row": 0,
 *     "snapshot": true
 * }
 * </pre>
 * <p>
 * The "{@code gtids}" field only appears in offsets produced when GTIDs are enabled. The "{@code snapshot}" field only appears in
 * offsets produced when the connector is in the middle of a snapshot. And finally, the "{@code ts}" field contains the
 * <em>seconds</em> since Unix epoch (since Jan 1, 1970) of the MySQL event; the message {@link Envelope envelopes} also have a
 * timestamp, but that timestamp is the <em>milliseconds</em> since since Jan 1, 1970.
 * <p>
 * Each change event envelope also includes the {@link #struct() source} struct that contains MySQL information about that
 * particular event, including a mixture the fields from the binlog filename and position where the event can be found,
 * and when GTIDs are enabled the GTID of the
 * transaction in which the event occurs. Like with the offset, the "{@code snapshot}" field only appears for events produced
 * when the connector is in the middle of a snapshot. Note that this information is likely different than the offset information,
 * since the connector may need to restart from either just after the most recently completed transaction or the beginning
 * of the most recently started transaction (whichever appears later in the binlog).
 * <p>
 * Here's a JSON-like representation of the source for the metadata for an event that corresponds to the above partition and
 * offset:
 *
 * <pre>
 * {
 *     "name": "production-server",
 *     "server_id": 112233,
 *     "ts_sec": 1465937,
 *     "gtid": "db58b0ae-2c10-11e6-b284-0242ac110002:199",
 *     "file": "mysql-bin.000003",
 *     "pos" = 1081,
 *     "row": 0,
 *     "snapshot": true,
 *     "thread" : 1,
 *     "db" : "inventory",
 *     "table" : "products"
 * }
 * </pre>
 *
 * @author Randall Hauch
 */
@NotThreadSafe
public final class SourceInfo extends BinlogSourceInfo {
    public SourceInfo(MySqlConnectorConfig connectorConfig) {
        super(connectorConfig);
    }
}
