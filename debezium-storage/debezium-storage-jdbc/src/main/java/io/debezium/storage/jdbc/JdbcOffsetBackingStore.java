/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.jdbc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;

/**
 * Implementation of OffsetBackingStore that saves data to database table.
 */
public class JdbcOffsetBackingStore implements OffsetBackingStore {

    public static final Field OFFSET_STORAGE_JDBC_URI = Field.create("offset.storage.jdbc.uri")
            .withDescription("URI of the database which will be used to record the database history")
            .withValidation(Field::isRequired);

    public static final Field OFFSET_STORAGE_JDBC_USER = Field.create("offset.storage.jdbc.user")
            .withDescription("Username of the database which will be used to record the database history")
            .withValidation(Field::isRequired);

    public static final Field OFFSET_STORAGE_JDBC_PASSWORD = Field.create("offset.storage.jdbc.password")
            .withDescription("Password of the database which will be used to record the database history")
            .withValidation(Field::isRequired);

    public static final String DEFAULT_OFFSET_STORAGE_TABLE_NAME = "debezium_offset_storage";
    public static final Field OFFSET_STORAGE_TABLE_NAME = Field.create("offset.storage.jdbc.offset_table_name")
            .withDescription("Name of the table to store offsets")
            .withDefault(DEFAULT_OFFSET_STORAGE_TABLE_NAME);

    public static final String OFFSET_STORAGE_TABLE_DDL = "CREATE TABLE %s(id VARCHAR(36) NOT NULL, offset_key VARCHAR(1255), offset_val VARCHAR(1255)," +
            "record_insert_ts TIMESTAMP NOT NULL," +
            "record_insert_seq INTEGER NOT NULL" +
            ")";

    public static final String OFFSET_STORAGE_TABLE_SELECT = "SELECT id, offset_key, offset_val FROM %s ORDER BY record_insert_ts, record_insert_seq";

    public static final String OFFSET_STORAGE_TABLE_INSERT = "INSERT INTO %s VALUES ( ?, ?, ?, ?, ? )";

    public static final String OFFSET_STORAGE_TABLE_DELETE = "DELETE FROM %s";

    private static final Logger LOG = LoggerFactory.getLogger(JdbcOffsetBackingStore.class);

    protected ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    protected ExecutorService executor;
    private final AtomicInteger recordInsertSeq = new AtomicInteger(0);
    private Connection conn;
    private String jdbcUri;

    private String offsetStorageTableName;

    public JdbcOffsetBackingStore() {
    }

    public String fromByteBuffer(ByteBuffer data) {
        return (data != null) ? String.valueOf(StandardCharsets.UTF_16.decode(data.asReadOnlyBuffer())) : null;
    }

    public ByteBuffer toByteBuffer(String data) {
        return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_16)) : null;
    }

    @Override
    public void configure(WorkerConfig config) {

        try {
            jdbcUri = config.getString("offset.storage.jdbc.uri");
            offsetStorageTableName = config.getString(OFFSET_STORAGE_TABLE_NAME.name());
            conn = DriverManager.getConnection(jdbcUri, config.getString(OFFSET_STORAGE_JDBC_USER.name()), config.getString(OFFSET_STORAGE_JDBC_PASSWORD.name()));
            conn.setAutoCommit(false);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to connect JDBC offset backing store: " + jdbcUri, e);
        }
    }

    @Override
    public synchronized void start() {
        executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));

        LOG.info("Starting JdbcOffsetBackingStore db {}", jdbcUri);
        try {
            initializeTable();
        }
        catch (SQLException e) {

            throw new IllegalStateException("Failed to create JDBC offset table: " + jdbcUri, e);
        }
        load();
    }

    private void initializeTable() throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet tableExists = dbMeta.getTables(null, null, offsetStorageTableName, null);

        if (tableExists.next()) {
            return;
        }

        LOG.debug("Creating table {} to store offset", offsetStorageTableName);
        conn.prepareStatement(String.format(OFFSET_STORAGE_TABLE_DDL, offsetStorageTableName)).execute();
    }

    protected void save() {
        try {
            LOG.debug("Saving data to state table...");
            try (PreparedStatement sqlDelete = conn.prepareStatement(String.format(OFFSET_STORAGE_TABLE_DELETE, offsetStorageTableName))) {
                sqlDelete.executeUpdate();
                for (Map.Entry<String, String> mapEntry : data.entrySet()) {
                    Timestamp currentTs = new Timestamp(System.currentTimeMillis());
                    String key = (mapEntry.getKey() != null) ? mapEntry.getKey() : null;
                    String value = (mapEntry.getValue() != null) ? mapEntry.getValue() : null;
                    // Execute a query
                    try (PreparedStatement sql = conn.prepareStatement(String.format(OFFSET_STORAGE_TABLE_INSERT, offsetStorageTableName))) {
                        sql.setString(1, UUID.randomUUID().toString());
                        sql.setString(2, key);
                        sql.setString(3, value);
                        sql.setTimestamp(4, currentTs);
                        sql.setInt(5, recordInsertSeq.incrementAndGet());
                        sql.executeUpdate();
                    }
                }
            }
            conn.commit();
        }
        catch (SQLException e) {
            try {
                conn.rollback();
            }
            catch (SQLException ex) {
                //
            }
            throw new ConnectException(e);
        }
    }

    private void load() {
        try {
            ConcurrentHashMap<String, String> tmpData = new ConcurrentHashMap<>();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(OFFSET_STORAGE_TABLE_SELECT, offsetStorageTableName));
            while (rs.next()) {
                String key = rs.getString("offset_key");
                String val = rs.getString("offset_val");
                tmpData.put(key, val);
            }
            data = tmpData;
        }
        catch (SQLException e) {
            LOG.error("Failed recover records from database: {}", jdbcUri, e);
        }
    }

    private void stopExecutor() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop JdbcOffsetBackingStore. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public synchronized void stop() {
        stopExecutor();
        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (SQLException e) {
            LOG.error("Exception while stopping JdbcOffsetBackingStore", e);
        }
        LOG.info("Stopped JdbcOffsetBackingStore");
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(new Callable<>() {
            @Override
            public Void call() {
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                    if (entry.getKey() == null) {
                        continue;
                    }
                    data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
                }
                save();
                if (callback != null) {
                    callback.onCompletion(null, null);
                }
                return null;
            }
        });
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(new Callable<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> call() {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                for (ByteBuffer key : keys) {
                    result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
                }
                return result;
            }
        });
    }
}
