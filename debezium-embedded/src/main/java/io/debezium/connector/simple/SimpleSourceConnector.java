/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.simple;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.debezium.config.Configuration;
import io.debezium.util.Collect;

/**
 * A very simple {@link SourceConnector} for testing that reliably produces the same records in the same order, and useful
 * for testing the infrastructure to run {@link SourceConnector}s.
 * <p>
 * This connector produces messages with keys having a single monotonically-increasing integer field named {@code id}:
 *
 * <pre>
 * {
 *     "id" : "1"
 * }
 * </pre>
 *
 *
 * and values with a {@code batch} field containing the 1-based batch number, a {@code record} field containing the
 * 1-based record number within the batch, and an optional {@code timestamp} field that contains a simulated number of
 * milliseconds past epoch computed by adding the start time of the connector task with the message {@code id}:
 *
 * <pre>
 * {
 *     "batch" : "1",
 *     "record" : "1",
 *     "timestamp" : null
 * }
 * </pre>
 *
 * In multitask configuration <pre>key.id</pre> has a positive offset equal to <pre>task.id * @{@link #TASK_VALUE_OFFSET}</pre>
 *  * E.g. with 3 task (0, 1, 2) the <pre>key.id</pre> of 1st record emitted by task with <pre>task.id=1</pre> is by default <pre>10_001</pre>
 *
 * @author Randall Hauch
 */
public class SimpleSourceConnector extends SourceConnector {

    protected static final String VERSION = "1.0";

    public static final String TOPIC_NAME = "topic.name";
    public static final String RECORD_COUNT_PER_BATCH = "record.count.per.batch";
    public static final String BATCH_COUNT = "batch.count";
    public static final String DEFAULT_TOPIC_NAME = "simple.topic";
    public static final String INCLUDE_TIMESTAMP = "include.timestamp";
    public static final String RETRIABLE_ERROR_ON = "error.retriable.on";
    public static final String INTERNAL_TASK_ID = "task.id";
    public static final String TASK_VALUE_OFFSET = "task.value.offset";
    public static final String INTERNAL_TASKS_MAX = "tasks.max";
    public static final int DEFAULT_RECORD_COUNT_PER_BATCH = 1;
    public static final int DEFAULT_BATCH_COUNT = 10;
    public static final boolean DEFAULT_INCLUDE_TIMESTAMP = false;
    public static final int DEFAULT_TASK_VALUE_OFFSET = 10_000;

    private Map<String, String> config;

    public SimpleSourceConnector() {
    }

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
        return SimpleSourceConnector.SimpleConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Configuration config = Configuration.from(this.config);
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = config.edit()
                    .with(INTERNAL_TASK_ID, i)
                    .with(INTERNAL_TASKS_MAX, maxTasks)
                    .build()
                    .asMap();

            configs.add(taskConfig);
        }
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

    public static class SimpleConnectorTask extends SourceTask {

        private static boolean isThrownErrorOnRecord;

        private int recordsPerBatch;
        private int errorOnRecord;
        private List<SourceRecord> records;
        private final AtomicBoolean running = new AtomicBoolean();

        @Override
        public String version() {
            return VERSION;
        }

        @Override
        public void start(Map<String, String> props) {
            if (running.compareAndSet(false, true)) {
                Configuration config = Configuration.from(props);
                recordsPerBatch = config.getInteger(RECORD_COUNT_PER_BATCH, DEFAULT_RECORD_COUNT_PER_BATCH);
                int batchCount = config.getInteger(BATCH_COUNT, DEFAULT_BATCH_COUNT);
                String topic = config.getString(TOPIC_NAME, DEFAULT_TOPIC_NAME);
                boolean includeTimestamp = config.getBoolean(INCLUDE_TIMESTAMP, DEFAULT_INCLUDE_TIMESTAMP);
                errorOnRecord = config.getInteger(RETRIABLE_ERROR_ON, -1);
                int taskId = config.getInteger(INTERNAL_TASK_ID, 0);
                int maxTasks = config.getInteger(INTERNAL_TASKS_MAX, 1);
                int valueOffset = config.getInteger(TASK_VALUE_OFFSET, DEFAULT_TASK_VALUE_OFFSET);

                // Create the partition and schemas ...
                Map<String, ?> partition = maxTasks > 1
                        ? Collect.hashMapOf("source", "simple", "task", taskId)
                        : Collect.hashMapOf("source", "simple");
                Schema keySchema = SchemaBuilder.struct()
                        .name("simple.key")
                        .field("id", Schema.INT32_SCHEMA)
                        .build();
                Schema valueSchema = SchemaBuilder.struct()
                        .name("simple.value")
                        .field("batch", Schema.INT32_SCHEMA)
                        .field("record", Schema.INT32_SCHEMA)
                        .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                        .build();

                // Read the offset ...
                Map<String, ?> lastOffset = context.offsetStorageReader().offset(partition);
                long lastId = lastOffset == null ? 0L : (Long) lastOffset.get("id");

                // Generate the records that we need ...
                records = new LinkedList<>();
                long initialTimestamp = System.currentTimeMillis();
                int id = valueOffset * taskId;
                for (int batch = 0; batch != batchCount; ++batch) {
                    for (int recordNum = 0; recordNum != recordsPerBatch; ++recordNum) {
                        ++id;
                        if (id <= lastId) {
                            // We already produced this record, so skip it ...
                            continue;
                        }
                        if (!running.get()) {
                            // the task has been stopped ...
                            return;
                        }
                        // We've not seen this ID yet, so create a record ...
                        Map<String, ?> offset = Collect.hashMapOf("id", id);
                        Struct key = new Struct(keySchema);
                        key.put("id", id);
                        Struct value = new Struct(valueSchema);
                        value.put("batch", batch + 1);
                        value.put("record", recordNum + 1);
                        if (includeTimestamp) {
                            value.put("timestamp", initialTimestamp + id);
                        }
                        SourceRecord record = new SourceRecord(partition, offset, topic, 1, keySchema, key, valueSchema, value);
                        records.add(record);
                    }
                }
            }
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            if (records.isEmpty()) {
                // block forever, as this thread will be interrupted if/when the task is stopped ...
                new CountDownLatch(1).await();
            }
            if (running.get()) {
                // Still running, so process whatever is in the queue ...
                List<SourceRecord> results = new ArrayList<>();
                int record = 0;
                while (record < recordsPerBatch && record < records.size()) {
                    final SourceRecord fetchedRecord = records.get(record);
                    final Integer id = ((Struct) (fetchedRecord.key())).getInt32("id");
                    if (id == errorOnRecord && !isThrownErrorOnRecord) {
                        isThrownErrorOnRecord = true;
                        throw new RetriableException("Error on record " + errorOnRecord);
                    }
                    results.add(fetchedRecord);
                    record++;
                }
                records.removeAll(results);
                return results;
            }
            // No longer running ...
            return null;
        }

        @Override
        public void stop() {
            // Request the task to stop and return immediately ...
            running.set(false);
        }
    }
}
