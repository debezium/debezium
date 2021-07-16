/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.storage;

import com.google.common.base.Splitter;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.*;
import io.debezium.util.Collect;
import io.debezium.util.FunctionalReadWriteLock;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A {@link DatabaseHistory} implementation that stores the schema history in a local file.
 *
 * @author Randall Hauch
 */
@ThreadSafe
public final class JdbcDatabaseHistory extends AbstractDatabaseHistory {

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "offset.storage.";
    public static final Field JDBC_URI = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "jdbc.uri")
            .withDescription("Uri of the database which will be used to record the database history")
            .withValidation(Field::isRequired);
    public static final Field JDBC_USER = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "jdbc.user")
            .withDescription("Username of the database which will be used to record the database history")
            .withValidation(Field::isRequired);
    public static final Field JDBC_PASSWORD = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "jdbc.password")
            .withDescription("Password of the database which will be used to record the database history")
            .withValidation(Field::isRequired);
    public static final String DATABASE_HISTORY_TABLE_NAME = "debezium_database_history";
    public static final String DATABASE_HISTORY_TABLE_DDL = "CREATE TABLE " + DATABASE_HISTORY_TABLE_NAME +
            "(" +
            "history_insert_epoch_ms VARCHAR(255) NOT NULL," +
            "history_data_part1 VARCHAR(65000)," +
            "history_data_part2 VARCHAR(65000)," +
            "history_data_part3 VARCHAR(65000)" +
            ")";

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(JDBC_USER);
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final AtomicBoolean running = new AtomicBoolean();
    Connection conn;
    String jdbc_uri;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
        logger.error("in configure");
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new ConnectException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        config.validateAndRecord(ALL_FIELDS, logger::error);
        if (running.get()) {
            throw new IllegalStateException("Database history already initialized db: " + config.getString(JDBC_URI));
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);

        try {
            jdbc_uri = config.getString(JDBC_URI.name());
            conn = DriverManager.getConnection(config.getString(JDBC_URI.name()), config.getString(JDBC_USER.name()), config.getString(JDBC_PASSWORD.name()));
        }
        catch (SQLException e) {
            throw new IllegalStateException("Failed to connect " + jdbc_uri);
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
                    throw new DatabaseHistoryException("Unable to create history table " + jdbc_uri + ": " + e.getMessage(), e);
                }
            }
        });
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (record == null) {
            return;
        }
        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }

            try {
                logger.error("Inserting records into the table...");
                String line = writer.write(record.document());
                String currentTimeMillis = Long.toString(System.currentTimeMillis());
                List<String> substrings = Splitter.fixedLength(65000).splitToList(line);
                if (substrings.size() >= 4) {
                    throw new DatabaseHistoryException("Failed to save history data!");
                }
                PreparedStatement sql = conn.prepareStatement("INSERT INTO " + DATABASE_HISTORY_TABLE_NAME + " VALUES ( ?, ?, ?, ? )");
                sql.setString(1, currentTimeMillis);
                sql.setString(2, substrings.size() >= 1 ? substrings.get(0) : "");
                sql.setString(3, substrings.size() >= 2 ? substrings.get(1) : "");
                sql.setString(4, substrings.size() >= 3 ? substrings.get(2) : "");
                sql.executeUpdate();
                // todo check length 4 and above very long ones!
            }
            catch (IOException | SQLException e) {
                logger.error("Failed to convert record to string: {}", record, e);
            }
        });
    }

    @Override
    public void stop() {
        running.set(false);
        super.stop();
        try {
            conn.close();
        }
        catch (SQLException e) {
            logger.error("Exception during stop", e);
        }
    }

    @Override
    protected synchronized void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> {
            try {
                if (exists()) {
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT history_insert_epoch_ms, history_data_part1, history_data_part2, history_data_part3 FROM " + DATABASE_HISTORY_TABLE_NAME);
                    while (rs.next()) {
                        String line1 = rs.getString("history_data_part1");
                        String line2 = rs.getString("history_data_part2");
                        String line3 = rs.getString("history_data_part3");
                        String line = line1 + line2 + line3;
                        if (!line.isEmpty()) {
                            records.accept(new HistoryRecord(reader.read(line)));
                        }
                    }
                }
            }
            catch (IOException | SQLException e) {
                logger.error("Failed to add recover records from jdbc history {}", jdbc_uri, e);
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
            logger.error("Failed to check history from db: {}", jdbc_uri, e);
        }
        return sExists;
    }

    @Override
    public boolean exists() {
        return storageExists();
    }

    @Override
    public String toString() {
        return "Jdbc database: " + (jdbc_uri != null ? jdbc_uri : "(unstarted)");
    }

    @Override
    public void initializeStorage() {
        try {
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet tableExists = dbMeta.getTables(null, null, DATABASE_HISTORY_TABLE_NAME, null);

            if (tableExists.next()) {
                return;
            }
            logger.debug("Creating table {} to store offset", DATABASE_HISTORY_TABLE_NAME);
            conn.prepareStatement(DATABASE_HISTORY_TABLE_DDL).execute();
            logger.info("Created table in given database...");
        }
        catch (SQLException e) {
            logger.error("Error initializing", e);
        }
    }

}
