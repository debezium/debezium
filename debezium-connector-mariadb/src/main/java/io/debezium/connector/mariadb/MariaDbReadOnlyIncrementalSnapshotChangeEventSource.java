/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.sql.SQLException;
import java.util.function.Consumer;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.connector.binlog.gtid.GtidSet;
import io.debezium.connector.mariadb.gtid.MariaDbGtidSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 *
 * @author Chris Cranford
 */
public class MariaDbReadOnlyIncrementalSnapshotChangeEventSource
        extends BinlogReadOnlyIncrementalSnapshotChangeEventSource<MariaDbPartition, MariaDbOffsetContext> {

    public MariaDbReadOnlyIncrementalSnapshotChangeEventSource(MariaDbConnectorConfig connectorConfig,
                                                               JdbcConnection jdbcConnection,
                                                               EventDispatcher<MariaDbPartition, TableId> dispatcher,
                                                               DatabaseSchema<?> databaseSchema,
                                                               Clock clock,
                                                               SnapshotProgressListener<MariaDbPartition> progressListener,
                                                               DataChangeEventListener<MariaDbPartition> dataChangeEventListener,
                                                               NotificationService<MariaDbPartition, MariaDbOffsetContext> notificationService) {
        super(connectorConfig, jdbcConnection, dispatcher, databaseSchema, clock, progressListener, dataChangeEventListener, notificationService);
    }

    @Override
    protected void getExecutedGtidSet(Consumer<GtidSet> watermark) {
        try {
            jdbcConnection.query("SHOW GLOBAL VARIABLES LIKE 'GTID_BINLOG_POS'", rs -> {
                if (rs.next()) {
                    if (rs.getMetaData().getColumnCount() > 0) {
                        final String gtidSet = rs.getString(2);
                        watermark.accept(new MariaDbGtidSet(gtidSet));
                    }
                    else {
                        throw new UnsupportedOperationException("Need to add support for executed GTIDs for versions prior to 5.6.5");
                    }
                }
            });
            jdbcConnection.commit();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

}
