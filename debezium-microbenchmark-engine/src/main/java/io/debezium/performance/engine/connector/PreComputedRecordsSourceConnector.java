/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.performance.engine.connector;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.util.Collect;

/**
 * Simple source connector which produces limited number of pre-computed records.
 * It's similar to {@link SimpleSourceConnector}, but adjusted for benchmarks so fixed amount of records is generated beforehand and only these pre-generated records
 * are sent to the consumer.
 *
 * @author vjuranek
 */
public class PreComputedRecordsSourceConnector extends SourceConnector {

    protected static final String VERSION = "1.0";

    public static final String TOPIC_NAME = "simple.topic";
    public static final String RECORD_COUNT_PER_BATCH = "record.count.per.batch";
    public static final String BATCH_COUNT = "batch.count";
    public static final int DEFAULT_RECORD_COUNT_PER_BATCH = 2048;
    public static final int DEFAULT_BATCH_COUNT = 100;

    private Map<String, String> config;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PreComputedRecordsTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    public static class PreComputedRecordsTask extends SourceTask {
        // records has to be static variable, otherwise JMH count also the time for generating the records
        private static List<SourceRecord> records = precomputeRecords(DEFAULT_BATCH_COUNT * DEFAULT_RECORD_COUNT_PER_BATCH);
        private final AtomicBoolean running = new AtomicBoolean();
        private int batchCount;
        private int recordsPerBatch;
        private int recordsFrom;
        private int currentBatch;
        private int recordsSent;

        @Override
        public String version() {
            return VERSION;
        }

        @Override
        public void start(Map<String, String> props) {
            if (running.compareAndSet(false, true)) {
                Configuration config = Configuration.from(props);
                recordsPerBatch = config.getInteger(RECORD_COUNT_PER_BATCH, DEFAULT_RECORD_COUNT_PER_BATCH);
                batchCount = config.getInteger(BATCH_COUNT, DEFAULT_BATCH_COUNT);
                recordsFrom = 0;
                currentBatch = 0;
                recordsSent = 0;
            }
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            if (running.get()) {
                if ((recordsFrom + (currentBatch + 1) * recordsPerBatch) >= DEFAULT_BATCH_COUNT * DEFAULT_RECORD_COUNT_PER_BATCH) {
                    recordsFrom = 0;
                    currentBatch = 0;
                }
                int recordsTo = Math.min((currentBatch + 1) * recordsPerBatch, records.size() - recordsFrom) - 1;
                if (recordsFrom > recordsTo) {
                    return null;
                }
                List<SourceRecord> batch = records.subList(recordsFrom, recordsTo);
                currentBatch++;
                recordsSent = recordsSent + batch.size();
                recordsFrom = recordsTo;
                return batch;
            }
            return null;
        }

        @Override
        public void stop() {
            running.set(false);
        }
    }

    private static List<SourceRecord> precomputeRecords(int numberOfRecords) {
        Schema keySchema = SchemaBuilder.struct()
                .name("simple.key")
                .field("id", Schema.INT32_SCHEMA)
                .build();
        Schema valueSchema = SchemaBuilder.struct()
                .name("simple.value")
                .field("name", Schema.STRING_SCHEMA)
                .field("surname", Schema.STRING_SCHEMA)
                .field("address", Schema.STRING_SCHEMA)
                .field("batch", Schema.INT32_SCHEMA)
                .field("record", Schema.INT32_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        List<SourceRecord> records = new LinkedList<>();
        Random random = new Random();
        long initialTimestamp = System.currentTimeMillis();
        for (int recordNum = 0; recordNum != numberOfRecords; recordNum++) {
            Struct key = new Struct(keySchema);
            key.put("id", recordNum);
            Struct value = new Struct(valueSchema);
            value.put("name", randomString(random, 10));
            value.put("surname", randomString(random, 20));
            value.put("address", randomString(random, 30));
            value.put("batch", 1);
            value.put("record", recordNum + 1);
            value.put("timestamp", initialTimestamp + recordNum);
            SourceRecord record = new SourceRecord(Collect.hashMapOf("source", "simple"), Collect.hashMapOf("id", recordNum), TOPIC_NAME, 1, keySchema, key, valueSchema,
                    value);
            records.add(record);
        }

        return records;
    }

    private static String randomString(Random random, int length) {
        final int ASCII_CHAR_START = 97;
        final int ASCII_CHAR_END = 123;

        return random.ints(ASCII_CHAR_START, ASCII_CHAR_END).limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
