/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc.history;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
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

    private Connection conn;
    private JdbcSchemaHistoryConfig config;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        this.config = new JdbcSchemaHistoryConfig(config);
        if (running.get()) {
            throw new IllegalStateException("Database history already initialized db: " + this.config.getJdbcUrl());
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);

        try {
            conn = DriverManager.getConnection(this.config.getJdbcUrl(), this.config.getUser(), this.config.getPassword());
            conn.setAutoCommit(false);
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
                if (conn == null) {
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
                String line = writer.write(record.document());
                Timestamp currentTs = new Timestamp(System.currentTimeMillis());
                List<String> substrings = split(line, 65000);
                int partSeq = 0;
                for (String dataPart : substrings) {
                    PreparedStatement sql = conn.prepareStatement(config.getTableInsert());
                    sql.setString(1, UUID.randomUUID().toString());
                    sql.setString(2, dataPart);
                    sql.setInt(3, partSeq);
                    sql.setTimestamp(4, currentTs);
                    sql.setInt(5, recordInsertSeq.incrementAndGet());
                    sql.execute();
                    partSeq++;
                }
                conn.commit();
            }
            catch (IOException | SQLException e) {
                try {
                    conn.rollback();
                }
                catch (SQLException ex) {
                    // ignore
                }
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
            conn.close();
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
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(config.getTableSelect());

                    while (rs.next()) {
                        String historyData = rs.getString("history_data");

                        if (historyData.isEmpty() == false) {
                            records.accept(new HistoryRecord(reader.read(historyData)));
                        }
                    }
                }
                else {
                    LOG.error("Storage does not exist when recovering records");
                }
            }
            catch (IOException | SQLException e) {
                throw new SchemaHistoryException("Failed to recover records", e);
            }
        });
    }

    @Override
    public boolean storageExists() {
        boolean sExists = false;
        try {
            DatabaseMetaData dbMeta = conn.getMetaData();
            String databaseName = config.getDatabaseName();
            ResultSet tableExists = dbMeta.getTables(databaseName,
                    null, config.getTableName(), null);
            if (tableExists.next()) {
                sExists = true;
            }
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Failed to check database history storage", e);
        }
        return sExists;
    }

    @Override
    public boolean exists() {

        if (!storageExists()) {
            return false;
        }

        boolean isExists = false;
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(config.getTableDataExistsSelect());
            while (rs.next()) {
                isExists = true;
            }
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Failed to recover records", e);
        }

        return isExists;
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
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet tableExists = dbMeta.getTables(null, null, config.getTableName(), null);

            if (tableExists.next()) {
                return;
            }
            LOG.info("Creating table {} to store database history", config.getTableName());
            conn.prepareStatement(config.getTableCreate()).execute();
            LOG.info("Created table in given database...");
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Error initializing Database history storage", e);
        }
    }
}
