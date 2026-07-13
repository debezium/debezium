/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis.history;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.redis.RedisClient;

public class RedisSchemaHistoryTest {

    private static final String VALID_RECORD_1 = "{\"source\":{\"server\":\"test\"},\"position\":{\"file\":\"binlog.000001\",\"pos\":100},\"ddl\":\"CREATE TABLE first (id INT)\"}";
    // truncated JSON, e.g. after a partial write to the stream
    private static final String CORRUPTED_RECORD = "{\"source\":{\"server\":\"test\"},\"position\":";
    private static final String VALID_RECORD_2 = "{\"source\":{\"server\":\"test\"},\"position\":{\"file\":\"binlog.000001\",\"pos\":200},\"ddl\":\"CREATE TABLE second (id INT)\"}";

    @Test
    @FixFor("debezium/dbz#2221")
    public void shouldSkipCorruptedRecordAndRecoverSubsequentOnes() {
        final RedisSchemaHistory history = new RedisSchemaHistory();
        history.configure(
                Configuration.create()
                        .with(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "redis.address", "localhost:6379")
                        .build(),
                null, SchemaHistoryListener.NOOP, false);
        history.setRedisClient(new XRangeRedisClient(List.of(
                Map.of("schema", VALID_RECORD_1),
                Map.of("schema", CORRUPTED_RECORD),
                Map.of("schema", VALID_RECORD_2))));

        final List<HistoryRecord> recovered = new ArrayList<>();
        history.recoverRecords(recovered::add);

        assertEquals(2, recovered.size(), "corrupted record should be skipped, all remaining records recovered");
        assertEquals("CREATE TABLE first (id INT)", recovered.get(0).document().getString(HistoryRecord.Fields.DDL_STATEMENTS));
        assertEquals("CREATE TABLE second (id INT)", recovered.get(1).document().getString(HistoryRecord.Fields.DDL_STATEMENTS));
    }

    private static class XRangeRedisClient implements RedisClient {

        private final List<Map<String, String>> entries;

        XRangeRedisClient(List<Map<String, String>> entries) {
            this.entries = entries;
        }

        @Override
        public List<Map<String, String>> xrange(String key) {
            return entries;
        }

        @Override
        public void disconnect() {
        }

        @Override
        public void close() {
        }

        @Override
        public String xadd(String key, Map<String, String> hash) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> xadd(List<SimpleEntry<String, Map<String, String>>> hashes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long xlen(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> hgetAll(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hset(byte[] key, byte[] field, byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long waitReplicas(int replicas, long timeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String info(String section) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String clientList() {
            throw new UnsupportedOperationException();
        }
    }
}
