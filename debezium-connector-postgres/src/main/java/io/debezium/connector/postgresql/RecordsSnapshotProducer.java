/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.util.PGmoney;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * Producer of {@link org.apache.kafka.connect.source.SourceRecord source records} from a database snapshot. Once completed,
 * this producer can optionally continue streaming records, using another {@link RecordsStreamProducer} instance.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class RecordsSnapshotProducer extends RecordsProducer {

    private static final String CONTEXT_NAME = "records-snapshot-producer";

    private final ExecutorService executorService;
    private final Optional<RecordsStreamProducer> streamProducer;

    private final AtomicReference<SourceRecord> currentRecord;
    private final Snapshotter snapshotter;

    public RecordsSnapshotProducer(PostgresTaskContext taskContext,
                                   SourceInfo sourceInfo,
                                   Snapshotter snapshotter) {
        super(taskContext, sourceInfo);
        executorService = Threads.newSingleThreadExecutor(PostgresConnector.class, taskContext.config().getLogicalName(), CONTEXT_NAME);
        currentRecord = new AtomicReference<>();
        this.snapshotter = snapshotter;
        if (snapshotter.shouldStream()) {
            // we need to create the stream producer here to make sure it creates the replication connection;
            // otherwise we can't stream back changes happening while the snapshot is taking place
            streamProducer = Optional.of(new RecordsStreamProducer(taskContext, sourceInfo));
        } else {
            streamProducer = Optional.empty();
        }
    }

    @Override
    protected void start(BlockingConsumer<ChangeEvent> eventConsumer, Consumer<Throwable> failureConsumer) {
        // MDC should be in inherited from parent to child threads
        LoggingContext.PreviousContext previousContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            CompletableFuture.runAsync(this::delaySnapshotIfNeeded, executorService)
                             .thenRunAsync(() -> this.takeSnapshot(eventConsumer), executorService)
                             .thenRunAsync(() -> this.startStreaming(eventConsumer, failureConsumer), executorService)
                             .exceptionally(e -> {
                                 logger.error("unexpected exception", e.getCause() != null ? e.getCause() : e);
                                 // always stop to clean up data
                                 stop();
                                 failureConsumer.accept(e);

                                 return null;
                             });
        } finally {
            previousContext.restore();
        }
    }

    private void delaySnapshotIfNeeded() {
        Duration delay = taskContext.getConfig().getSnapshotDelay();
        if (delay.isZero() || delay.isNegative()) {
            return;
        }

        Threads.Timer timer = Threads.timer(Clock.SYSTEM, delay);
        Metronome metronome = Metronome.parker(ConfigurationDefaults.RETURN_CONTROL_INTERVAL, Clock.SYSTEM);

        while (!timer.expired()) {
            try {
                logger.info("The connector will wait for {}s before proceeding", timer.remaining().getSeconds());
                metronome.pause();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.debug("Interrupted while awaiting initial snapshot delay");
                return;
            }
        }
    }

    private void startStreaming(BlockingConsumer<ChangeEvent> consumer, Consumer<Throwable> failureConsumer) {
        try {
            // and then start streaming if necessary
            streamProducer.ifPresent(producer -> {
                if (sourceInfo.lsn() != null) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Snapshot finished, continuing streaming changes from {}", ReplicationConnection.format(sourceInfo.lsn()));
                    }
                }

                // still starting the stream producer, also if the connector has stopped already.
                // otherwise the executor started in its constructor wouldn't be stopped. This logic
                // will be obsolete when moving to the new framework classes.
                producer.start(consumer, failureConsumer);
            });
        } finally {
            // always cleanup our local data
            cleanup();
        }
    }

    @Override
    protected void commit(long lsn)  {
        streamProducer.ifPresent(x -> x.commit(lsn));
    }

    @Override
    protected void stop() {
        try {
            streamProducer.ifPresent(RecordsStreamProducer::stop);
        } finally {
            cleanup();
        }
    }

    private void cleanup() {
        currentRecord.set(null);
        executorService.shutdownNow();
    }

    private void takeSnapshot(BlockingConsumer<ChangeEvent> consumer) {
        if (executorService.isShutdown()) {
            logger.info("Not taking snapshot as this task has been cancelled already");
            return;
        }

        long snapshotStart = clock().currentTimeInMillis();
        Connection jdbcConnection = null;
        try (PostgresConnection connection = taskContext.createConnection()) {
            jdbcConnection = connection.connection();
            String lineSeparator = System.lineSeparator();

            logger.info("Step 0: disabling autocommit");
            connection.setAutoCommit(false);

            long lockTimeoutMillis = taskContext.config().snapshotLockTimeoutMillis();
            logger.info("Step 1: starting transaction and refreshing the DB schemas for database '{}' and user '{}'",
                        connection.database(), connection.username());
            // we're using the same isolation level that pg_backup uses
            StringBuilder statements = new StringBuilder("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;");
            connection.executeWithoutCommitting(statements.toString());
            statements.delete(0, statements.length());

            //next refresh the schema which will load all the tables taking the filters into account
            PostgresSchema schema = schema();
            schema.refresh(connection, false);

            logger.info("Step 2: locking each of the database tables, waiting a maximum of '{}' seconds for each lock",
                        lockTimeoutMillis / 1000d);
            statements.append("SET lock_timeout = ").append(lockTimeoutMillis).append(";").append(lineSeparator);
            // we're locking in SHARE UPDATE EXCLUSIVE MODE to avoid concurrent schema changes while we're taking the snapshot
            // this does not prevent writes to the table, but prevents changes to the table's schema....
            // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
            schema.tableIds().forEach(tableId -> statements.append("LOCK TABLE ")
                                                         .append(tableId.toDoubleQuotedString())
                                                         .append(" IN SHARE UPDATE EXCLUSIVE MODE;")
                                                         .append(lineSeparator));
            connection.executeWithoutCommitting(statements.toString());

            //now that we have the locks, refresh the schema
            schema.refresh(connection, false);

            long xlogStart = connection.currentXLogLocation();
            long txId = connection.currentTransactionId().longValue();
            if (logger.isInfoEnabled()) {
                logger.info("\t read xlogStart at '{}' from transaction '{}'", ReplicationConnection.format(xlogStart), txId);
            }

            // and mark the start of the snapshot
            sourceInfo.startSnapshot();
            // use the old xmin, as we don't want to update it if in xmin recovery
            sourceInfo.update(xlogStart, clock().currentTime(), txId, null, sourceInfo.xmin());

            logger.info("Step 3: reading and exporting the contents of each table");
            AtomicInteger rowsCounter = new AtomicInteger(0);

            for(TableId tableId : schema.tableIds()) {
                long exportStart = clock().currentTimeInMillis();
                logger.info("\t exporting data from table '{}'", tableId);
                try {
                    final Optional<String> selectStatement = snapshotter.buildSnapshotQuery(tableId);
                    if (!selectStatement.isPresent()) {
                        logger.warn("For table '{}' the select statement was not provided, skipping table", tableId);
                    }
                    else {
                        logger.info("For table '{}' using select statement: '{}'", tableId, selectStatement);

                        connection.queryWithBlockingConsumer(selectStatement.get(),
                                this::readTableStatement,
                                rs -> readTable(tableId, rs, consumer, rowsCounter));
                        if (logger.isInfoEnabled()) {
                            logger.info("\t finished exporting '{}' records for '{}'; total duration '{}'", rowsCounter.get(),
                                        tableId, Strings.duration(clock().currentTimeInMillis() - exportStart));
                        }
                        rowsCounter.set(0);
                    }
                } catch (SQLException e) {
                    throw new ConnectException(e);
                }
            }

            // finally commit the transaction to release all the locks...
            logger.info("Step 4: committing transaction '{}'", txId);
            jdbcConnection.commit();

            SourceRecord currentRecord = this.currentRecord.get();
            if (currentRecord != null) {
                // process and send the last record after marking it as such
                logger.info("Step 5: sending the last snapshot record");
                sourceInfo.markLastSnapshotRecord();

                // the sourceInfo element already has been baked into the record value, so
                // update the "last_snapshot_marker" in there
                changeSourceToLastSnapshotRecord(currentRecord);
                this.currentRecord.set(new SourceRecord(currentRecord.sourcePartition(), sourceInfo.offset(),
                                                        currentRecord.topic(), currentRecord.kafkaPartition(),
                                                        currentRecord.keySchema(), currentRecord.key(),
                                                        currentRecord.valueSchema(), currentRecord.value()));

                sendCurrentRecord(consumer);
            }

            // and complete the snapshot
            sourceInfo.completeSnapshot();
            if (logger.isInfoEnabled()) {
                logger.info("Snapshot completed in '{}'", Strings.duration(clock().currentTimeInMillis() - snapshotStart));
            }
            Heartbeat
                .create(
                    taskContext.config().getConfig(),
                    taskContext.topicSelector().getHeartbeatTopic(),
                    taskContext.config().getLogicalName()
                )
                .forcedBeat(
                    sourceInfo.partition(),
                    sourceInfo.offset(),
                    r -> consumer.accept(new ChangeEvent(r, sourceInfo.lsn())
                )
            );

            taskContext.schema().assureNonEmptySchema();
        }
        catch (SQLException e) {
            rollbackTransaction(jdbcConnection);

            throw new ConnectException(e);
        }
        catch(InterruptedException e)  {
            Thread.interrupted();
            rollbackTransaction(jdbcConnection);

            if (logger.isWarnEnabled()) {
                logger.warn("Snapshot aborted after '{}'", Strings.duration(clock().currentTimeInMillis() - snapshotStart));
            }
        }
    }

    private void changeSourceToLastSnapshotRecord(SourceRecord currentRecord) {
        final Struct envelope = (Struct) currentRecord.value();
        final Struct source = (Struct) envelope.get(Envelope.FieldName.SOURCE);
        if (source.schema().field(SourceInfo.LAST_SNAPSHOT_RECORD_KEY) != null && source.getBoolean(SourceInfo.LAST_SNAPSHOT_RECORD_KEY) != null) {
            source.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, true);
        }
        if (SnapshotRecord.fromSource(source) == SnapshotRecord.TRUE) {
            SnapshotRecord.LAST.toSource(source);
        }
    }

    private void rollbackTransaction(Connection jdbcConnection) {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.rollback();
            }
        }
        catch (SQLException se) {
            logger.error("Cannot rollback snapshot transaction", se);
        }
    }

    private Statement readTableStatement(Connection conn) throws SQLException {
        int fetchSize = taskContext.config().getSnapshotFetchSize();
        Statement statement = conn.createStatement(); // the default cursor is FORWARD_ONLY
        statement.setFetchSize(fetchSize);
        return statement;
    }

    private void readTable(TableId tableId, ResultSet rs,
                           BlockingConsumer<ChangeEvent> consumer,
                           AtomicInteger rowsCounter) throws SQLException, InterruptedException {
        Table table = schema().tableFor(tableId);
        assert table != null;
        final int numColumns = table.columns().size();
        final Object[] row = new Object[numColumns];
        final ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            rowsCounter.incrementAndGet();
            sendCurrentRecord(consumer);
            for (int i = 0, j = 1; i != numColumns; ++i, ++j) {
                row[i] = valueForColumn(rs, j, metaData);
            }
            generateReadRecord(tableId, row);
        }
    }

    private Object valueForColumn(ResultSet rs, int colIdx, ResultSetMetaData metaData) throws SQLException {
        try {
            final String columnTypeName = metaData.getColumnTypeName(colIdx);
            final PostgresType type = taskContext.schema().getTypeRegistry().get(columnTypeName);

            logger.trace("Type of incoming data is: {}", type.getOid());
            logger.trace("ColumnTypeName is: {}", columnTypeName);
            logger.trace("Type is: {}", type);

            if (type.isArrayType()) {
                return rs.getArray(colIdx);
            }

            switch (type.getOid()) {
                case PgOid.MONEY:
                    //TODO author=Horia Chiorean date=14/11/2016 description=workaround for https://github.com/pgjdbc/pgjdbc/issues/100
                    return new PGmoney(rs.getString(colIdx)).val;
                case PgOid.BIT:
                    return rs.getString(colIdx);
                case PgOid.NUMERIC:
                    final String s = rs.getString(colIdx);
                    if (s == null) {
                        return s;
                    }

                    Optional<SpecialValueDecimal> value = PostgresValueConverter.toSpecialValue(s);
                    return value.isPresent() ? value.get() : new SpecialValueDecimal(rs.getBigDecimal(colIdx));
                case PgOid.TIME:
                    // To handle time 24:00:00 supported by TIME columns, read the column as a string.
                case PgOid.TIMETZ:
                    // In order to guarantee that we resolve TIMETZ columns with proper microsecond precision,
                    // read the column as a string instead and then re-parse inside the converter.
                    return rs.getString(colIdx);
                default:
                    Object x = rs.getObject(colIdx);
                    if(x != null) {
                        logger.trace("rs getobject returns class: {}; rs getObject value is: {}", x.getClass(), x);
                    }
                    return x;
            }
        }
        catch (SQLException e) {
            // not a known type
            return rs.getObject(colIdx);
        }
    }

    protected void generateReadRecord(TableId tableId, Object[] rowData) {
        // Clear the existing record to prevent reprocessing stale data.
        currentRecord.set(null);

        if (rowData.length == 0) {
            return;
        }
        logger.trace("tableId value is: {}", tableId);
        TableSchema tableSchema = schema().schemaFor(tableId);
        assert tableSchema != null;
        Object key = tableSchema.keyFromColumnData(rowData);
        Struct value = tableSchema.valueFromColumnData(rowData);

        if (value == null) {
            logger.trace("Read event for null key with value {}", value);
            return;
        }
        Schema keySchema = tableSchema.keySchema();
        sourceInfo.update(clock().currentTimeAsInstant(), tableId);
        Map<String, ?> partition = sourceInfo.partition();
        Map<String, ?> offset = sourceInfo.offset();
        String topicName = topicSelector().topicNameFor(tableId);
        Envelope envelope = tableSchema.getEnvelopeSchema();
        currentRecord.set(new SourceRecord(partition, offset, topicName, null, keySchema, key, envelope.schema(),
                                           envelope.read(value, sourceInfo.struct(), clock().currentTimeInMillis())));
    }

    private void sendCurrentRecord(BlockingConsumer<ChangeEvent> consumer) throws InterruptedException {
        SourceRecord record = currentRecord.get();
        if (record == null) {
            return;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("sending read event '{}'", record);
        }
        //send the last generated record
        consumer.accept(new ChangeEvent(record, sourceInfo.lsn()));
    }

    boolean isStreamingRunning() {
        return streamProducer.isPresent() && streamProducer.get().isStreamingRunning();
    }
}
