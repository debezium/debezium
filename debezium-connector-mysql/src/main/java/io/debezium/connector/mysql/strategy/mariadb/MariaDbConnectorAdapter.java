/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.strategy.mariadb;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventData;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MariaDbProtocolFieldReader;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import io.debezium.connector.mysql.strategy.AbstractHistoryRecordComparator;
import io.debezium.connector.mysql.strategy.BinaryLogClientConfigurator;
import io.debezium.connector.mysql.strategy.ConnectorAdapter;
import io.debezium.connector.mysql.strategy.mysql.MySqlConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * This connector adapter provides a complete implementation for MariaDB assuming that
 * the MariaDB driver is used for connections.
 *
 * @author Chris Cranford
 */
public class MariaDbConnectorAdapter implements ConnectorAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbConnectorAdapter.class);

    private final MySqlConnectorConfig connectorConfig;
    private final MariaDbBinaryLogClientConfigurator binaryLogClientConfigurator;

    public MariaDbConnectorAdapter(MySqlConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
        this.binaryLogClientConfigurator = new MariaDbBinaryLogClientConfigurator(connectorConfig);
    }

    @Override
    public AbstractConnectorConnection createConnection(Configuration configuration) {
        MariaDbConnectionConfiguration connectionConfig = new MariaDbConnectionConfiguration(configuration);
        return new MariaDbConnection(connectionConfig, new MariaDbProtocolFieldReader(connectorConfig));
    }

    @Override
    public BinaryLogClientConfigurator getBinaryLogClientConfigurator() {
        return binaryLogClientConfigurator;
    }

    @Override
    public void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(MySqlOffsetContext offsetContext,
                                                                        AbstractConnectorConnection connection)
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
            else if (!connectorConfig.getSnapshotMode().shouldStream()) {
                LOGGER.warn("Failed retrieving binlog position, continuing as streaming CDC wasn't requested");
            }
            else {
                throw new DebeziumException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
            }
        });
    }

    @Override
    public String getRecordingQueryFromEvent(EventData eventData) {
        return ((AnnotateRowsEventData) eventData).getRowsQuery();
    }

    @Override
    public String getJavaEncodingForCharSet(String charSetName) {
        // todo: this should use a MariaDB specific implementation
        return MySqlConnection.getJavaEncodingForCharSet(charSetName);
    }

    @Override
    public AbstractHistoryRecordComparator getHistoryRecordComparator() {
        return new MariaDbHistoryRecordComparator(connectorConfig.gtidSourceFilter());
    }

    @Override
    public <T> IncrementalSnapshotContext<T> getIncrementalSnapshotContext() {
        if (connectorConfig.isReadOnlyConnection()) {
            return new MariaDbReadOnlyIncrementalSnapshotContext<>();
        }
        return new SignalBasedIncrementalSnapshotContext<>();
    }

    @Override
    public <T> IncrementalSnapshotContext<T> loadIncrementalSnapshotContextFromOffset(Map<String, ?> offset) {
        if (connectorConfig.isReadOnlyConnection()) {
            return MariaDbReadOnlyIncrementalSnapshotContext.load(offset);
        }
        return SignalBasedIncrementalSnapshotContext.load(offset);
    }

    @Override
    public Long getReadOnlyIncrementalSnapshotSignalOffset(MySqlOffsetContext previousOffsets) {
        return ((MariaDbReadOnlyIncrementalSnapshotContext<TableId>) previousOffsets.getIncrementalSnapshotContext()).getSignalOffset();
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
        return new MariaDbReadOnlyIncrementalSnapshotChangeEventSource<>(
                connectorConfig,
                connection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener,
                notificationService);
    }
}
