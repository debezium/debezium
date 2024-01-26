/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mysql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlBinaryProtocolFieldReader;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlFieldReader;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlTextProtocolFieldReader;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.AbstractHistoryRecordComparator;
import io.debezium.connector.mysql.strategy.BinaryLogClientConfigurator;
import io.debezium.connector.mysql.strategy.ConnectorAdapter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * This connector adapter provides a complete implementation for MySQL assuming that the
 * MySQL driver is used for connections.
 *
 * @author Chris Cranford
 */
public class MySqlConnectorAdapter implements ConnectorAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorAdapter.class);

    private final MySqlConnectorConfig connectorConfig;
    private final MySqlBinaryLogClientConfigurator binaryLogClientConfigurator;

    public MySqlConnectorAdapter(MySqlConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.binaryLogClientConfigurator = new MySqlBinaryLogClientConfigurator(connectorConfig);
    }

    @Override
    public AbstractConnectorConnection createConnection(Configuration configuration) {
        final MySqlConnectionConfiguration connectionConfig = new MySqlConnectionConfiguration(configuration);
        return new MySqlConnection(connectionConfig, resolveFieldReader());
    }

    @Override
    public BinaryLogClientConfigurator getBinaryLogClientConfigurator() {
        return binaryLogClientConfigurator;
    }

    @Override
    public void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(MySqlOffsetContext offsetContext,
                                                                        AbstractConnectorConnection connection,
                                                                        SnapshotterService snapshotterService)
            throws Exception {
        LOGGER.info("Read binlog position of MySQL primary server");
        final String showMasterStmt = "SHOW MASTER STATUS";
        connection.query(showMasterStmt, rs -> {
            if (rs.next()) {
                final String binlogFilename = rs.getString(1);
                final long binlogPosition = rs.getLong(2);
                offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    final String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                    offsetContext.setCompletedGtidSet(gtidSet);
                    LOGGER.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                            gtidSet);
                }
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
    public String getJavaEncodingForCharSet(String charSetName) {
        return MySqlConnection.getJavaEncodingForCharSet(charSetName);
    }

    @Override
    public String getRecordingQueryFromEvent(EventData eventData) {
        return ((RowsQueryEventData) eventData).getQuery();
    }

    @Override
    public AbstractHistoryRecordComparator getHistoryRecordComparator() {
        return new MySqlHistoryRecordComparator(connectorConfig.gtidSourceFilter());
    }

    @Override
    public <T> IncrementalSnapshotContext<T> getIncrementalSnapshotContext() {
        if (connectorConfig.isReadOnlyConnection()) {
            return new MySqlReadOnlyIncrementalSnapshotContext<>();
        }
        return new SignalBasedIncrementalSnapshotContext<>();
    }

    @Override
    public <T> IncrementalSnapshotContext<T> loadIncrementalSnapshotContextFromOffset(Map<String, ?> offset) {
        if (connectorConfig.isReadOnlyConnection()) {
            return MySqlReadOnlyIncrementalSnapshotContext.load(offset);
        }
        return SignalBasedIncrementalSnapshotContext.load(offset);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Long getReadOnlyIncrementalSnapshotSignalOffset(MySqlOffsetContext previousOffsets) {
        return ((MySqlReadOnlyIncrementalSnapshotContext<TableId>) previousOffsets.getIncrementalSnapshotContext()).getSignalOffset();
    }

    @Override
    public IncrementalSnapshotChangeEventSource<MySqlPartition, ? extends DataCollectionId> createIncrementalSnapshotChangeEventSource(
                                                                                                                                       MySqlConnectorConfig connectorConfig,
                                                                                                                                       AbstractConnectorConnection connection,
                                                                                                                                       EventDispatcher<MySqlPartition, ? extends DataCollectionId> dispatcher,
                                                                                                                                       MySqlDatabaseSchema schema,
                                                                                                                                       Clock clock,
                                                                                                                                       SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
                                                                                                                                       DataChangeEventListener<MySqlPartition> dataChangeEventListener,
                                                                                                                                       NotificationService<MySqlPartition, MySqlOffsetContext> notificationService) {
        return new MySqlReadOnlyIncrementalSnapshotChangeEventSource<>(
                connectorConfig,
                connection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService);
    }

    private MySqlFieldReader resolveFieldReader() {
        // todo: this null check is needed for the connection validation (try to rework)
        return connectorConfig != null && connectorConfig.useCursorFetch()
                ? new MySqlBinaryProtocolFieldReader(connectorConfig)
                : new MySqlTextProtocolFieldReader(connectorConfig);
    }

}
