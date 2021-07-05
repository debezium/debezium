/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.debezium.server.storage;

import io.debezium.config.Field;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Implementation of OffsetBackingStore that saves data locally to a file. To ensure this behaves
 * similarly to a real backing store, operations are executed asynchronously on a background thread.
 */
public class JdbcOffsetBackingStore implements OffsetBackingStore {
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
    private static final Logger log = LoggerFactory.getLogger(JdbcOffsetBackingStore.class);
    public static final String OFFSET_STORAGE_TABLE_NAME = "debezium_offset_storage";
    public static final String OFFSET_STORAGE_TABLE_DDL = "CREATE TABLE " + OFFSET_STORAGE_TABLE_NAME +
            "(" +
            "offset_insert_epoch_ms VARCHAR(255) NOT NULL," +
            "offset_key VARCHAR(1255)," +
            "offset_val VARCHAR(1255)" +
            ")";
    protected Map<String, String> data = new HashMap<>();
    protected ExecutorService executor;
    Connection conn;
    String jdbc_uri;

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
            jdbc_uri = config.getString(JDBC_URI.name());
            conn = DriverManager.getConnection(jdbc_uri, config.getString(JDBC_USER.name()), config.getString(JDBC_PASSWORD.name()));
            log.error("Failed to connect Jdbc offset backing store");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to connect Jdbc offset backing store: " + jdbc_uri, e);
        }
    }

    @Override
    public synchronized void start() {
        executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));

        log.info("Starting JdbcOffsetBackingStore db {}", jdbc_uri);
        try {
            initializeTable();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to create Jdbc offset table: " + jdbc_uri, e);
        }
        load();
    }

    private void initializeTable() throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet tableExists = dbMeta.getTables(null, null, OFFSET_STORAGE_TABLE_NAME, null);

        if (tableExists.next()) {
            return;
        }
        log.debug("Creating table {} to store offset", OFFSET_STORAGE_TABLE_NAME);
        conn.prepareStatement(OFFSET_STORAGE_TABLE_DDL).execute();
    }

    protected void save() {
        String currentTimeMillis = Long.toString(System.currentTimeMillis());
        try {
            log.debug("Saving data to state table...");
            conn.setAutoCommit(false);
            PreparedStatement sqlDelete = conn.prepareStatement("DELETE FROM " + OFFSET_STORAGE_TABLE_NAME);
            sqlDelete.executeUpdate();
            for (Map.Entry<String, String> mapEntry : data.entrySet()) {
                String key = (mapEntry.getKey() != null) ? mapEntry.getKey() : null;
                String value = (mapEntry.getValue() != null) ? mapEntry.getValue() : null;
                // Execute a query
                PreparedStatement sql = conn.prepareStatement("INSERT INTO " + OFFSET_STORAGE_TABLE_NAME + " VALUES ( ?, ?,? )");
                sql.setString(1, currentTimeMillis);
                sql.setString(2, key);
                sql.setString(3, value);
                sql.executeUpdate();
            }
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (SQLException ex) {
                //
            }
            throw new ConnectException(e);
        }
    }


    private void load() {
        try {
            Map<String, String> tmpData = new HashMap<>();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT offset_insert_epoch_ms, offset_key, offset_val FROM " + OFFSET_STORAGE_TABLE_NAME);
            while (rs.next()) {
                String key = rs.getString("offset_key");
                String val = rs.getString("offset_val");
                tmpData.put(key, val);
            }
            data = tmpData;
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Failed recover records from database: {}", jdbc_uri, e);
        }
    }

    private void stopExecutor() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
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
            conn.close();
        } catch (SQLException e) {
            log.error("Exception while stopping JdbcOffsetBackingStore", e);
        }
        log.info("Stopped JdbcOffsetBackingStore");
    }


    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                    if (entry.getKey() == null) {
                        continue;
                    }
                    data.put(fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
                }
                save();
                if (callback != null)
                    callback.onCompletion(null, null);
                return null;
            }
        });
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(new Callable<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public Map<ByteBuffer, ByteBuffer> call() throws Exception {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
                for (ByteBuffer key : keys) {
                    result.put(key, toByteBuffer(data.get(fromByteBuffer(key))));
                }
                return result;
            }
        });
    }
}
