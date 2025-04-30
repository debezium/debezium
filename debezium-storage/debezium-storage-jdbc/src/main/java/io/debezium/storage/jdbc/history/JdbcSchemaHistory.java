/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.jdbc.RetriableConnection;
import io.debezium.util.FunctionalReadWriteLock;

/**
 * A {@link SchemaHistory} implementation that stores the schema history to database table
 *
 * @author Ismail Simsek
 */
@ThreadSafe
@Incubating
public final class JdbcSchemaHistory extends AbstractSchemaHistory {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSchemaHistory.class);

    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger recordInsertSeq = new AtomicInteger(0);
    private RetriableConnection conn;
    private JdbcSchemaHistoryConfig config;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = new JdbcSchemaHistoryConfig(config);
        if (running.get()) {
            throw new IllegalStateException("Database history already initialized db: " + this.config.getJdbcUrl());
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);

        try {
            conn = new RetriableConnection(this.config.getJdbcUrl(), this.config.getUser(), this.config.getPassword(),
                    this.config.getWaitRetryDelay(), this.config.getMaxRetryCount());
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to connect " + this.config.getJdbcUrl(), e);
        }
    }

    @Override
    public void start() {
        super.start();
        lock.write(() -> {
            if (running.compareAndSet(false, true)) {
                if (!conn.isOpen()) {
                    throw new IllegalStateException("Database connection must be set before it is started");
                }
                try {
                    if (!storageExists()) {
                        initializeStorage();
                    }
                }
                catch (Exception e) {
                    throw new SchemaHistoryException("Unable to create history table " + config.getJdbcUrl() + ": " + e.getMessage(), e);
                }
            }
        });
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (record == null) {
            return;
        }
        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }

            try {
                conn.executeWithRetry(conn -> {
                    String line = null;
                    try {
                        line = writer.write(record.document());
                    }
                    catch (IOException e) {
                        throw new DebeziumException(e);
                    }
                    Timestamp currentTs = new Timestamp(System.currentTimeMillis());
                    List<String> substrings = split(line, 65000);
                    int partSeq = 0;
                    for (String dataPart : substrings) {
                        try (PreparedStatement sql = conn.prepareStatement(config.getTableInsert())) {
                            sql.setString(1, UUID.randomUUID().toString());
                            sql.setString(2, dataPart);
                            sql.setInt(3, partSeq);
                            sql.setTimestamp(4, currentTs);
                            sql.setInt(5, recordInsertSeq.incrementAndGet());
                            sql.executeUpdate();
                            partSeq++;
                        }
                    }
                    conn.commit();
                }, "store history record", true);
            }
            catch (SQLException e) {
                throw new SchemaHistoryException("Failed to store record: " + record, e);
            }
        });
    }

    private static List<String> split(String s, int chunkSize) {
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < s.length(); i += chunkSize) {
            chunks.add(s.substring(i, Math.min(s.length(), i + chunkSize)));
        }
        return chunks;
    }

    @Override
    public void stop() {
        running.set(false);
        super.stop();
        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (SQLException e) {
            LOG.error("Exception during stop", e);
        }
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            try {
                if (exists()) {
                    conn.executeWithRetry(conn -> {
                        try (
                                Statement stmt = conn.createStatement();
                                ResultSet rs = stmt.executeQuery(config.getTableSelect())) {
                            StringBuilder historyDataBuilder = new StringBuilder();
                            boolean isNotFirst = false;
                            while (rs.next()) {
                                // warning: No continuity check - nice to have
                                int recordInsertSeq = rs.getInt("record_insert_seq");
                                String historyDataSpc = rs.getString("history_data");
                                // Reduce if else execution
                                if (isNotFirst) {
                                    // must be ordered by ts asc !!!
                                    // check if the prev record is the completed json
                                    if (recordInsertSeq == 0) {
                                        try {
                                            records.accept(new HistoryRecord(reader.read(historyDataBuilder.toString())));
                                        } catch (IOException e) {
                                            throw new DebeziumException(e);
                                        }
                                        // clear
                                        historyDataBuilder.setLength(0);
                                        // continue
                                    }
                                    historyDataBuilder.append(historyDataSpc);
                                } else {
                                    // first record
                                    historyDataBuilder.append(historyDataSpc);
                                    isNotFirst = true;
                                }
                            }
                            // the last record  - must not be empty
                            if (!historyDataBuilder.isEmpty()) {
                                try {
                                    records.accept(new HistoryRecord(reader.read(historyDataBuilder.toString())));
                                } catch (IOException e) {
                                    throw new DebeziumException(e);
                                }
                            }
                        }
                    }, "recover history records", false);
                }
                else {
                    LOG.error("Storage does not exist when recovering records");
                }
            }
            catch (SQLException e) {
                throw new SchemaHistoryException("Failed to recover records", e);
            }
        });
    }

    @Override
    public boolean storageExists() {
        try {
            return conn.executeWithRetry(conn -> {
                boolean exists = false;
                DatabaseMetaData dbMeta = conn.getMetaData();

                String databaseName = config.getDatabaseName();
                try (ResultSet tableExists = dbMeta.getTables(databaseName,
                        null, config.getTableName(), null)) {
                    if (tableExists.next()) {
                        exists = true;
                    }
                    return exists;
                }
            }, "history storage exists", false);
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Failed to check database history storage", e);
        }
    }

    @Override
    public boolean exists() {

        if (!storageExists()) {
            return false;
        }

        try {
            return conn.executeWithRetry(conn -> {
                boolean isExists = false;
                try (
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(config.getTableDataExistsSelect());) {
                    while (rs.next()) {
                        isExists = true;
                    }
                    return isExists;
                }
            }, "history records exist check", false);
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Failed to recover records", e);
        }
    }

    @VisibleForTesting
    JdbcSchemaHistoryConfig getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "Jdbc database: " + (config != null ? config.getJdbcUrl() : "(unstarted)");
    }

    @Override
    public void initializeStorage() {
        try {
            conn.executeWithRetry(conn -> {
                DatabaseMetaData dbMeta = conn.getMetaData();
                try (ResultSet tableExists = dbMeta.getTables(null, null, config.getTableName(), null)) {
                    if (tableExists.next()) {
                        return;
                    }
                    LOG.info("Creating table {} to store database history", config.getTableName());
                    try (var ps = conn.prepareStatement(config.getTableCreate())) {
                        ps.execute();
                        LOG.info("Created table in given database...");
                    }
                    conn.commit();
                }
            }, "initialize storage", false);
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Error initializing Database history storage", e);
        }
    }

}
