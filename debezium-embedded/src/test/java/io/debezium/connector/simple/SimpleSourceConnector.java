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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
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
 * @author Randall Hauch
 */
public class SimpleSourceConnector extends SourceConnector {

    protected static final String VERSION = "1.0";

    public static final Field TOPIC_NAME = Field.create("topic.name").withDefault("simple.topic");
    public static final Field RECORD_COUNT_PER_BATCH = Field.create("record.count.per.batch").withDefault(1);
    public static final Field BATCH_COUNT = Field.create("batch.count").withDefault(10);
    public static final Field INCLUDE_TIMESTAMP = Field.create("include.timestamp").withDefault(false);
    public static final Field TASK_COUNT = Field.create("task.count").withDefault(1);
    protected static final Field TASK_ID = Field.create("task.id").withDefault(1);

    /**
     * Includes only the public fields
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(TOPIC_NAME, RECORD_COUNT_PER_BATCH, BATCH_COUNT, INCLUDE_TIMESTAMP, TASK_COUNT);

    private Logger logger = LoggerFactory.getLogger(getClass());
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
        Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new ConnectException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SimpleSourceConnector.SimpleConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Configuration config = Configuration.from(this.config);
        int taskCount = config.getInteger(TASK_COUNT);
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i != taskCount; ++i) {
            configs.add(taskConfigForTask(config, i + 1, taskCount).asMap());
        }
        return configs;
    }

    protected Configuration taskConfigForTask(Configuration config, int taskNumber, int taskCount) {
        return config.edit().with(TASK_ID, taskNumber).build();
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    public static class SimpleConnectorTask extends SourceTask {

        private Logger logger = LoggerFactory.getLogger(getClass());
        private int recordsPerBatch;
        private Queue<SourceRecord> records;
        protected final AtomicBoolean running = new AtomicBoolean();

        @Override
        public String version() {
            return VERSION;
        }

        @Override
        public void start(Map<String, String> props) {
            if (running.compareAndSet(false, true)) {
                Configuration config = Configuration.from(props);
                if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
                    throw new ConnectException(
                            "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
                }
                preStart(config);
                recordsPerBatch = config.getInteger(RECORD_COUNT_PER_BATCH);
                int batchCount = config.getInteger(BATCH_COUNT);
                String topic = config.getString(TOPIC_NAME);
                boolean includeTimestamp = config.getBoolean(INCLUDE_TIMESTAMP);

                // Create the partition and schemas ...
                Map<String, ?> partition = Collect.hashMapOf("source", "simple");
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
                int id = 0;
                for (int batch = 0; batch != batchCount; ++batch) {
                    preBatch(batch);
                    for (int recordNum = 0; recordNum != recordsPerBatch; ++recordNum) {
                        ++id;
                        preRecord(recordNum, id);
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
                        postRecord(recordNum, id, record);
                    }
                    postBatch(batch);
                }
            }
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            while (running.get() && records.isEmpty()) {
                // block for a while ...
                Thread.sleep(100);
            }
            if (running.get()) {
                // Still running but something in the queue ...
                List<SourceRecord> results = new ArrayList<>();
                int record = 0;
                while (record < recordsPerBatch && !records.isEmpty()) {
                    results.add(records.poll());
                }
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

        /**
         * Called during {@link #start(Map)} but after only the configuration has been validated.
         * 
         * @param config the validated task configuration
         */
        protected void preStart(Configuration config) {
        }

        /**
         * Called before each batch is started.
         * 
         * @param batchNumber the 0-based batch number
         */
        protected void preBatch(int batchNumber) {
        }

        /**
         * Called after each batch is completed.
         * 
         * @param batchNumber the 0-based batch number
         */
        protected void postBatch(int batchNumber) {
        }

        /**
         * Called before each record is created.
         * 
         * @param recordNumber the 0-based record number in the batch
         * @param recordId the unique ID of the record that will be created
         */
        protected void preRecord(int recordNumber, long recordId) {
        }

        /**
         * Called after each record is created and enqueued.
         * 
         * @param recordNumber the 0-based record number in the batch
         * @param recordId the unique ID of the record
         * @param record the source record
         */
        protected void postRecord(int recordNumber, long recordId, SourceRecord record) {
        }
    }
}
