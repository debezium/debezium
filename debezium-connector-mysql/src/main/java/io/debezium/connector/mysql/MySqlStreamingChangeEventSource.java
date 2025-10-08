/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.network.SSLMode;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 *
 * @author Jiri Pechanec
 */
public class MySqlStreamingChangeEventSource extends BinlogStreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlStreamingChangeEventSource.class);

    private final MySqlConnectorConfig connectorConfig;
    private GtidSet gtidSet;

    public MySqlStreamingChangeEventSource(MySqlConnectorConfig connectorConfig,
                                           BinlogConnectorConnection connection,
                                           EventDispatcher<MySqlPartition, TableId> dispatcher,
                                           ErrorHandler errorHandler,
                                           Clock clock,
                                           MySqlTaskContext taskContext,
                                           MySqlDatabaseSchema schema,
                                           MySqlStreamingChangeEventSourceMetrics metrics,
                                           SnapshotterService snapshotterService) {
        super(connectorConfig, connection, dispatcher, errorHandler, clock, taskContext, schema, metrics, snapshotterService);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected void setEventTimestamp(Event event, long eventTs) {
        if (eventTimestamp == null || !isGtidModeEnabled()) {
            // Fallback to second resolution event timestamps
            eventTimestamp = Instant.ofEpochMilli(eventTs);
        }
        else if (event.getHeader().getEventType() == EventType.GTID) {
            // Prefer higher resolution replication timestamps from MySQL 8 GTID events, if possible
            GtidEventData gtidEvent = unwrapData(event);
            final long gtidEventTs = gtidEvent.getOriginalCommitTimestamp();
            if (gtidEventTs != 0) {
                // >= MySQL 8.0.1, prefer the higher resolution replication timestamp
                eventTimestamp = Instant.EPOCH.plus(gtidEventTs, ChronoUnit.MICROS);
            }
            else {
                // Fallback to second resolution event timestamps
                eventTimestamp = Instant.ofEpochMilli(eventTs);
            }
        }
    }

    /**
     * Handle the supplied event with a {@link GtidEventData} that signals the beginning of a GTID transaction.
     * We don't yet know whether this transaction contains any events we're interested in, but we have to record
     * it so that we know the position of this event and know we've processed the binlog to this point.
     * <p>
     * Note that this captures the current GTID and complete GTID set, regardless of whether the connector is
     * {@link MySqlConnectorConfig#getGtidSourceFilter() filtering} the GTID set upon connection. We do this because
     * we actually want to capture all GTID set values found in the binlog, whether or not we process them.
     * However, only when we connect do we actually want to pass to MySQL only those GTID ranges that are applicable
     * per the configuration.
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the GTID event to be processed; may not be null
     * @param gtidSourceFilter the GTID source filter
     */
    @Override
    protected void handleGtidEvent(MySqlPartition partition, MySqlOffsetContext offsetContext, Event event,
                                   Predicate<String> gtidSourceFilter) {
        LOGGER.debug("GTID transaction: {}", event);
        GtidEventData gtidEvent = unwrapData(event);
        String gtid = gtidEvent.getGtid();
        gtidSet.add(gtid);
        offsetContext.startGtid(gtid, gtidSet.toString()); // rather than use the client's GTID set
        setIgnoreDmlEventByGtidSource(false);
        if (gtidSourceFilter != null && gtid != null) {
            String uuid = gtid.trim().substring(0, gtid.indexOf(":"));
            if (!gtidSourceFilter.test(uuid)) {
                setIgnoreDmlEventByGtidSource(true);
            }
        }
        setGtidChanged(gtid);
    }

    /**
     * Handle the supplied event with an {@link RowsQueryEventData} or {@link AnnotateRowsEventData} by
     * recording the original SQL query that generated the event.
     *
     * @param event the database change data event to be processed; may not be null
     */
    @Override
    protected void handleRecordingQuery(MySqlOffsetContext offsetContext, Event event) {
        final EventData eventData = unwrapData(event);
        if (eventData instanceof RowsQueryEventData) {
            final String query = ((RowsQueryEventData) eventData).getQuery();
            offsetContext.setQuery(query);
        }
    }

    @Override
    public void init(MySqlOffsetContext offsetContext) {
        setEffectiveOffsetContext(offsetContext != null ? offsetContext : MySqlOffsetContext.initial(connectorConfig));
    }

    @Override
    protected Class<? extends SourceConnector> getConnectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected EventType getIncludeQueryEventType() {
        return EventType.ROWS_QUERY;
    }

    @Override
    protected EventType getGtidEventType() {
        return EventType.GTID;
    }

    @Override
    protected void initializeGtidSet(String value) {
        this.gtidSet = new GtidSet(value);
    }

    @Override
    protected SSLMode sslModeFor(BinlogConnectorConfig.SecureConnectionMode mode) {
        switch ((MySqlConnectorConfig.MySqlSecureConnectionMode) mode) {
            case DISABLED:
                return SSLMode.DISABLED;
            case PREFERRED:
                return SSLMode.PREFERRED;
            case REQUIRED:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_IDENTITY:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }

}
