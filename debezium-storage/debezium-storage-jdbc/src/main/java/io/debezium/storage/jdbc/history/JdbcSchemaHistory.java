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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.util.Collect;
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

    public static final Field JDBC_URI = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "jdbc.uri")
            .withDescription("URI of the database which will be used to record the database history")
            .withValidation(Field::isRequired);

    public static final Field JDBC_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "jdbc.user")
            .withDescription("Username of the database which will be used to record the database history")
            .withValidation(Field::isRequired);

    public static final Field JDBC_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "jdbc.password")
            .withDescription("Password of the database which will be used to record the database history")
            .withValidation(Field::isRequired);

    private static final String DATABASE_HISTORY_TABLE_NAME = "debezium_database_history";

    /**
     * Table that will store database history.
     * id - Unique identifier(UUID)
     * history_data - Schema history data.
     * history_data_seq - Schema history part sequence number.
     * record_insert_ts - Timestamp when the record was inserted
     * record_insert_seq - Sequence number(Incremented for every record inserted)
     */
    private static final String DATABASE_HISTORY_TABLE_DDL = "CREATE TABLE " + DATABASE_HISTORY_TABLE_NAME +
            "(" +
            "id VARCHAR(36) NOT NULL," +
            "history_data VARCHAR(65000)," +
            "history_data_seq INTEGER," +
            "record_insert_ts TIMESTAMP NOT NULL," +
            "record_insert_seq INTEGER NOT NULL" +
            ")";

    private static final String DATABASE_HISTORY_TABLE_SELECT = "SELECT id, history_data, history_data_seq FROM " + DATABASE_HISTORY_TABLE_NAME
            + " ORDER BY record_insert_ts, record_insert_seq, id, history_data_seq";

    private static final String DATABASE_HISTORY_TABLE_DATA_EXISTS_SELECT = "SELECT * FROM " + DATABASE_HISTORY_TABLE_NAME + " LIMIT 1";

    private static final String DATABASE_HISTORY_TABLE_INSERT = "INSERT INTO " + DATABASE_HISTORY_TABLE_NAME + " VALUES ( ?, ?, ?, ?, ? )";

    private static final Collection<Field> ALL_FIELDS = Collect.arrayListOf(JDBC_USER);

    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger recordInsertSeq = new AtomicInteger(0);

    private Connection conn;
    private String jdbcUri;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        if (!config.validateAndRecord(ALL_FIELDS, LOG::error)) {
            throw new ConnectException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        config.validateAndRecord(ALL_FIELDS, LOG::error);
        if (running.get()) {
            throw new IllegalStateException("Database history already initialized db: " + config.getString(JDBC_URI));
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);

        try {
            jdbcUri = config.getString(JDBC_URI.name());
            conn = DriverManager.getConnection(config.getString(JDBC_URI.name()), config.getString(JDBC_USER.name()), config.getString(JDBC_PASSWORD.name()));
            conn.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to connect " + jdbcUri);
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
                    throw new SchemaHistoryException("Unable to create history table " + jdbcUri + ": " + e.getMessage(), e);
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
                    PreparedStatement sql = conn.prepareStatement(DATABASE_HISTORY_TABLE_INSERT);
                    sql.setString(1, UUID.randomUUID().toString());
                    sql.setString(2, dataPart);
                    sql.setInt(3, partSeq);
                    sql.setTimestamp(4, currentTs);
                    sql.setInt(5, recordInsertSeq.incrementAndGet());
                    sql.executeUpdate();
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
                    ResultSet rs = stmt.executeQuery(DATABASE_HISTORY_TABLE_SELECT);

                    while (rs.next()) {
                        String historyData = rs.getString("history_data");

                        if (historyData.isEmpty() == false) {
                            records.accept(new HistoryRecord(reader.read(historyData)));
                        }
                    }
                }
                else {
                    // System.out.println("STORAGE DOES NOT EXIST");
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
            ResultSet tableExists = dbMeta.getTables(null, null, DATABASE_HISTORY_TABLE_NAME, null);
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
            ResultSet rs = stmt.executeQuery(DATABASE_HISTORY_TABLE_DATA_EXISTS_SELECT);
            while (rs.next()) {
                isExists = true;
            }
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Failed to recover records", e);
        }

        return isExists;
    }

    @Override
    public String toString() {
        return "Jdbc database: " + (jdbcUri != null ? jdbcUri : "(unstarted)");
    }

    @Override
    public void initializeStorage() {
        try {
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet tableExists = dbMeta.getTables(null, null, DATABASE_HISTORY_TABLE_NAME, null);

            if (tableExists.next()) {
                return;
            }
            LOG.debug("Creating table {} to store database history", DATABASE_HISTORY_TABLE_NAME);
            conn.prepareStatement(DATABASE_HISTORY_TABLE_DDL).execute();
            LOG.info("Created table in given database...");
        }
        catch (SQLException e) {
            throw new SchemaHistoryException("Error initializing Database history storage", e);
        }
    }
}
