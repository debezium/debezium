/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSnapshotChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mariadb.metrics.MariaDbSnapshotChangeEventSourceMetrics;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * A {@link io.debezium.pipeline.source.spi.SnapshotChangeEventSource} implementation for MariaDB.
 *
 * @author Chris Cranford
 */
public class MariaDbSnapshotChangeEventSource extends BinlogSnapshotChangeEventSource<MariaDbPartition, MariaDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbSnapshotChangeEventSource.class);

    private final MariaDbConnectorConfig connectorConfig;

    public MariaDbSnapshotChangeEventSource(MariaDbConnectorConfig connectorConfig,
                                            MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                            MariaDbDatabaseSchema schema,
                                            EventDispatcher<MariaDbPartition, TableId> dispatcher,
                                            Clock clock,
                                            MariaDbSnapshotChangeEventSourceMetrics metrics,
                                            BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
                                            Runnable preSnapshotAction,
                                            NotificationService<MariaDbPartition, MariaDbOffsetContext> notificationService,
                                            SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics, lastEventProcessor,
                preSnapshotAction, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected MariaDbOffsetContext getInitialOffsetContext(BinlogConnectorConfig connectorConfig) {
        return MariaDbOffsetContext.initial((MariaDbConnectorConfig) connectorConfig);
    }

    @Override
    protected void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(MariaDbOffsetContext offsetContext,
                                                                           BinlogConnectorConnection connection,
                                                                           SnapshotterService snapshotterService)
            throws Exception {
        LOGGER.info("Read binlog position of MariaDB primary server");
        final String showMasterStmt = "SHOW MASTER STATUS";
        connection.query(showMasterStmt, rs -> {
            if (rs.next()) {
                final String binlogFilename = rs.getString(1);
                final long binlogPosition = rs.getLong(2);
                offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);

                connection.query("SHOW GLOBAL VARIABLES LIKE 'GTID_BINLOG_POS'", rs2 -> {
                    if (rs2.next() && rs2.getMetaData().getColumnCount() > 0) {
                        final String gtidSet = rs2.getString(2);
                        offsetContext.setCompletedGtidSet(gtidSet);
                        LOGGER.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition, gtidSet);
                    }
                    else {
                        LOGGER.info("\t using binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                    }
                });
            }
            else if (!snapshotterService.getSnapshotter().shouldStream()) {
                LOGGER.warn("Failed retrieving binlog position, continuing as streaming CDC wasn't requested");
            }
            else {
                throw new DebeziumException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
            }
        });
    }

    @Override
    protected MariaDbOffsetContext copyOffset(RelationalSnapshotContext<MariaDbPartition, MariaDbOffsetContext> snapshotContext) {
        return new MariaDbOffsetContext.Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
