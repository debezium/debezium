/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.time.Instant;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import com.google.re2j.Pattern;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource;
import io.debezium.connector.binlog.BinlogTaskContext;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mariadb.metrics.MariaDbStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * @author Chris Cranford
 */
public class MariaDbStreamingChangeEventSource extends BinlogStreamingChangeEventSource<MariaDbPartition, MariaDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MariaDbStreamingChangeEventSource.class);

    private final MariaDbConnectorConfig connectorConfig;
    private final TableId signalDataCollectionId;
    private MariadbGtidSet gtidSet;

    public MariaDbStreamingChangeEventSource(MariaDbConnectorConfig connectorConfig,
                                             BinlogConnectorConnection connection,
                                             EventDispatcher<MariaDbPartition, TableId> dispatcher,
                                             ErrorHandler errorHandler,
                                             Clock clock,
                                             MariaDbTaskContext taskContext,
                                             MariaDbStreamingChangeEventSourceMetrics metrics,
                                             SnapshotterService snapshotterService) {
        super(connectorConfig, connection, dispatcher, errorHandler, clock, taskContext, taskContext.getSchema(), metrics, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.signalDataCollectionId = getSignalDataCollectionId(connectorConfig);
    }

    @Override
    public void init(MariaDbOffsetContext offsetContext) {
        setEffectiveOffsetContext(offsetContext != null ? offsetContext : MariaDbOffsetContext.initial(connectorConfig));
    }

    @Override
    protected Class<? extends SourceConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }

    @Override
    protected BinaryLogClient createBinaryLogClient(BinlogTaskContext<?> taskContext,
                                                    BinlogConnectorConfig connectorConfig,
                                                    Map<String, Thread> clientThreads,
                                                    BinlogConnectorConnection connection) {
        final BinaryLogClient client = super.createBinaryLogClient(taskContext, connectorConfig, clientThreads, connection);
        if (connectorConfig.isSqlQueryIncluded()) {
            // Binlog client explicitly needs to be told to enable ANNOTATE_ROWS events, which is the MariaDB
            // equivalent of ROWS_QUERY for MySQL. This must be done ahead of the connection to make sure that
            // the right negotiation bits are set during the handshake.
            client.setUseSendAnnotateRowsEvent(true);
        }
        return client;
    }

    @Override
    protected void configureReplicaCompatibility(BinaryLogClient client) {
        client.setMariaDbSlaveCapability(4);
    }

    @Override
    protected void setEventTimestamp(Event event, long eventTs) {
        eventTimestamp = Instant.ofEpochMilli(eventTs);
    }

    @Override
    protected void handleGtidEvent(MariaDbPartition partition, MariaDbOffsetContext offsetContext, Event event,
                                   Predicate<String> gtidDmlSourceFilter)
            throws InterruptedException {
        LOGGER.debug("MariaDB GTID transaction: {}", event);

        // NOTE: MariadbGtidEventData (GTID_EVENT) does not include the server id in the event data payload.
        // We need to manually construct the GTID combining the data from the GTID_EVENT payload and header.
        MariadbGtidEventData gtidEvent = unwrapData(event);
        String gtid = String.format("%d-%d-%d", gtidEvent.getDomainId(), event.getHeader().getServerId(), gtidEvent.getSequence());

        // String gtid = gtidEvent.toString();
        gtidSet.add(gtid);
        offsetContext.startGtid(gtid, gtidSet.toString());

        setIgnoreDmlEventByGtidSource(false);
        if (gtidDmlSourceFilter != null && gtid != null) {
            String uuid = gtidEvent.getDomainId() + "-" + gtidEvent.getServerId();
            if (!gtidDmlSourceFilter.test(uuid)) {
                setIgnoreDmlEventByGtidSource(true);
            }
        }
        setGtidChanged(gtid);

        // With compatibility mode 4, this event equates to a new transaction.
        handleTransactionBegin(partition, offsetContext, event, null);
    }

    @Override
    protected void handleRecordingQuery(MariaDbOffsetContext offsetContext, Event event) {
        final EventData eventData = unwrapData(event);
        if (eventData instanceof AnnotateRowsEventData) {
            final String query = ((AnnotateRowsEventData) eventData).getRowsQuery();
            // todo: Cache ANNOTATE_ROWS query with events
            // During incremental snapshots, the updates made to the signal table can lead to a case where
            // the query stored in the offsets mismatch the events being dispatched.
            //
            // IncrementalSnapshotIT#updates performs a series of updates where the pk/aa columns are changed
            // i.e. [1,0] to [1,2000] and the ANNOTATE_ROWS event that conatins the query specifies this SQL:
            // "UPDATE `schema`.`a` SET aa = aa + 2000 WHERE pk > 0 and pk <= 10"
            //
            // The problem is that signal events do not seem to record a query string in the offsets for MySQL
            // but this gets recorded for MariaDB, and causes a mismatch of query string values that differs
            // from MySQL's behavior. For now, this allows the tests to pass.
            if (signalDataCollectionId != null) {
                if (query.toLowerCase().contains(signalDataCollectionId.toQuotedString('`').toLowerCase())) {
                    return;
                }
            }
            offsetContext.setQuery(query);
        }
    }

    @Override
    protected EventType getIncludeQueryEventType() {
        return EventType.ANNOTATE_ROWS;
    }

    @Override
    protected EventType getGtidEventType() {
        return EventType.MARIADB_GTID;
    }

    @Override
    protected void initializeGtidSet(String value) {
        this.gtidSet = new MariadbGtidSet(value);
    }

    @Override
    protected SSLMode sslModeFor(BinlogConnectorConfig.SecureConnectionMode mode) {
        switch ((MariaDbConnectorConfig.MariaDbSecureConnectionMode) mode) {
            case DISABLE:
                return SSLMode.DISABLED;
            case TRUST:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_FULL:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }

    private static TableId getSignalDataCollectionId(MariaDbConnectorConfig connectorConfig) {
        if (!Strings.isNullOrBlank(connectorConfig.getSignalingDataCollectionId())) {
            return TableId.parse(connectorConfig.getSignalingDataCollectionId());
        }
        return null;
    }
}
