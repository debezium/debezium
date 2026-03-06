/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.config.CommonConnectorConfig.TASKS_MAX;
import static io.debezium.connector.mongodb.MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED;
import static io.debezium.connector.mongodb.MongoDbConnectorConfig.MONGODB_MULTI_TASK_GEN;
import static io.debezium.connector.mongodb.MongoDbConnectorConfig.MONGODB_MULTI_TASK_HOP_SECONDS;
import static io.debezium.connector.mongodb.MongoDbConnectorConfig.MONGODB_MULTI_TASK_PREV_GEN;
import static io.debezium.connector.mongodb.MongoDbConnectorConfig.MONGODB_MULTI_TASK_PREV_TASKS;
import static io.debezium.connector.mongodb.SourceInfo.ORDER;
import static io.debezium.connector.mongodb.SourceInfo.TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

public class MongoDbMultiTaskConnectorScalingIT extends AbstractMongoConnectorIT {

    private static final String db = "mongodb_multitask_scaling";
    private static final String col = "collection";

    @Before
    public void beforeEach() {
        initializeConnectorTestFramework();

        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .with(MongoDbConnectorConfig.CAPTURE_MODE, "change_streams_with_pre_image")
                .with(MONGODB_MULTI_TASK_HOP_SECONDS, 1)
                .build();

        context = new MongoDbTaskContext(config);
        TestHelper.cleanDatabase(mongo, db);
    }

    @After
    public void afterEach() {
        stopConnector();
    }

    /**
     * Verifies that the connector doesn't run unless the current generation is greater than the previous generation.
     * This does not actually connect to the Mongo server.
     */
    @Test
    public void shouldNotStartWithInvalidPrevGenConfig() {
        config = Configuration.create()
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .with(MONGODB_MULTI_TASK_PREV_GEN, 0)
                .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so expected to see the following error in the log: " +
                "The 'mongodb.multi.prev.gen' value true is invalid: current multi-task generation must be greater than previous " +
                "multi-task generation. received 0 and 0 as current and previous generations respectively");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    /**
     * Verifies that the connector doesn't run unless the previous tasks field is greater than 0. This does not actually
     * connect to the Mongo server.
     */
    @Test
    public void shouldNotStartWithInvalidPrevTasksConfig() {
        config = Configuration.create()
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, 0)
                .build();

        // we expect the engine will log at least one error, so preface it ...
        logger.info("Attempting to start the connector with an INVALID configuration, so expected to see the following error in the log: " +
                "The 'mongodb.multi.prev.task' value '0' is invalid: A positive integer is expected");
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    /**
     * Verifies that multi-task connectors fail to start up if previous generation's offsets are missing.
     */
    @Test
    public void multiTaskConnectorsShouldFailIfOffsetsAreMissing() {
        // Start connector for the first time with non-negative previous generation
        int numTasks = 2;
        int generation = 1;
        int numPrevTasks = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, generation - 1)
                .build();
        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    /**
     * Verifies that multi-task connectors fail to start up if previous generation is missing some, but not all, of its offsets.
     *
     * 1. Create offset for one task in generation 0 (a generation with two tasks)
     * 2. Attempt to start up connector in generation 1 and assert that it fails
     *
     * @throws Exception
     */
    @Test
    public void multiTaskConnectorsShouldFailIfSingleOffsetIsMissing() throws Exception {
        int numTasks = 2;
        int generation = 0;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .build();

        // Create offset
        MongoDbPartition task0Partition = new MongoDbPartition("mongo", null, true, generation, 0, numTasks);
        Map<String, Object> offset = Collect.hashMapOf(
                TIMESTAMP, 0,
                ORDER, 0);
        Map<Map<String, ?>, Map<String, ?>> offsets = Collect.hashMapOf(task0Partition.getSourcePartition(), offset);
        storeOffsets(config, offsets);

        // Attempt to increment generation
        int numPrevTasks = numTasks;
        ++generation;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, generation - 1)
                .build();

        start(MongoDbConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }

    /**
     * Verifies that multi-task connectors use the oldest available offset from the previous generation.
     *
     * 1. Create offsets for two tasks in the previous generation. One offset is set in the past and one is set in the future.
     * 2. Insert records
     * 3. Start multi-task connector with 2 tasks
     * 4. Allow connector to consume records and assert that it consumes all records
     * @throws Exception
     */
    @Test
    public void multiTaskConnectorsShouldGetOldestOffsetFromPreviousGenerationOnScale() throws Exception {
        int numTasks = 2;
        int generation = 0;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .build();

        // Create offsets
        MongoDbPartition task0Partition = new MongoDbPartition("mongo", null, true, generation, 0, numTasks);
        Map<String, Object> futureOffset = Collect.hashMapOf(
                TIMESTAMP, 2147483647, // TODO: update this test before Jan 2038 :)
                ORDER, 0);
        MongoDbPartition task1Partition = new MongoDbPartition("mongo", null, true, generation, 1, numTasks);
        Map<String, Object> oldOffset = Collect.hashMapOf(
                TIMESTAMP, Instant.now().getEpochSecond(),
                ORDER, 0);

        Map<Map<String, ?>, Map<String, ?>> offsets = Collect.hashMapOf(
                task0Partition.getSourcePartition(), futureOffset,
                task1Partition.getSourcePartition(), oldOffset);
        storeOffsets(config, offsets);

        int numPrevTasks = 2;
        generation = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_GEN, 0)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .build();
        // Insert records
        int numRecords = 100;
        populateDataCollectionOverTime(numRecords, 5000, 100);

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo", 0);
        waitForStreamingRunning("mongodb", "mongo", 1);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);
        for (SourceRecord record : inserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }
    }

    /**
     * Verifies that multi-task connectors skip resolving an offset from the previous generation if offsets for their task/generation
     * already exist.
     *
     * 1. Start multi-task connector for the first time
     * 2. Allow connector to consume records and publish offset
     * 3. Start connector in the next generation
     * 4. Allow connector to consume records and publish offset
     * 5. Restart connector with same multi-task configs (taskId, generation, prevTasks) and allow it to consume records
     * 6. Assert that final connector consumes records starting from second connector's offset
     *
     * @throws Exception
     */
    @Test
    public void multiTaskConnectorsShouldNotGetExistingOffsetFromPreviousGeneration() throws Exception {
        int numTasks = 1;
        int generation = 0;
        int numPrevTasks = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .build();

        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        // Insert records
        int numRecords = 100;
        populateDataCollection(numRecords);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);

        // Stop connector
        stopConnector();

        // Add new records
        numRecords = 10;
        populateDataCollection(numRecords);

        // Increment generation
        numPrevTasks = 1;
        generation = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, generation - 1)
                .build();
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Verify that connector starts consuming records from previous generation's offset
        SourceRecords newInserts = consumeRecordsByTopic(numRecords);
        assertThat(newInserts.allRecordsInOrder().size()).isEqualTo(numRecords);

        // Restart connector with new records
        stopConnector();
        populateDataCollection(numRecords);
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Verify that connector starts consuming records from previously recorded offset in this generation
        SourceRecords newestInserts = consumeRecordsByTopic(numRecords);
        assertThat(newestInserts.allRecordsInOrder().size()).isEqualTo(numRecords);
    }

    /**
     * Verifies that multi-task connectors skip resolving an offset from old non-multitask offsets if offsets for their task/generation
     * already exist.
     *
     * 1. Start non-multitask connector
     * 2. Allow connector to consume records and publish offset
     * 3. Start multi-task connector for the first time with negative previous generation
     * 4. Allow connector to consume records and publish offset
     * 5. Restart connector with same multi-task configs (taskId, generation, prevTasks) and allow it to consume records
     * 6. Assert that final connector consumes records starting from second connector's offset
     *
     * @throws Exception
     */
    @Test
    public void multiTaskConnectorShouldNotGetExistingOffsetFromNonMultiTaskOffsets() throws Exception {
        // Start connector with multitask disabled
        config = config.edit()
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, false)
                .build();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        int numRecords = 100;
        populateDataCollection(numRecords);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);

        // Stop connector
        stopConnector();

        // Start multitask connector
        int numTasks = 2;
        int numPrevTasks = 1;
        int generation = 0;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, -1)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .build();
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Add new records and verify that only the new records are consumed
        populateDataCollectionOverTime(numRecords, 5000, 100);
        SourceRecords newInserts = consumeRecordsByTopic(numRecords);
        assertThat(newInserts.allRecordsInOrder().size()).isEqualTo(numRecords);
        for (SourceRecord record : newInserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }

        // Restart connector with new records
        stopConnector();
        populateDataCollectionOverTime(numRecords, 5000, 100);
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Verify that connector starts consuming records from previously recorded offset in this generation
        SourceRecords newestInserts = consumeRecordsByTopic(numRecords);
        assertThat(newestInserts.allRecordsInOrder().size()).isEqualTo(numRecords);
        for (SourceRecord record : newestInserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }
    }

    /**
     * Verifies that multi-task connectors use old non-multitask offsets on scale up/down when the previous generation is
     * negative.
     *
     * 1. Start non-multitask connector
     * 2. Allow connector to consume records and publish offset
     * 3. Start multi-task connector for the first time with negative previous generation
     * 4. Allow connector to consume records and publish offset
     * 5. Assert that connector consumes records starting from first connector's offset
     *
     * @throws Exception
     */
    @Test
    public void multiTaskConnectorShouldGetNonMultiTaskOffsetsOnNegativePreviousGeneration() throws Exception {
        // Start connector with multitask disabled
        config = config.edit()
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, false)
                .build();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        int numRecords = 100;
        populateDataCollection(numRecords);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);

        // Stop connector
        stopConnector();

        // Add new records
        populateDataCollectionOverTime(numRecords, 5000, 100);

        // Start multitask connector
        int numTasks = 2;
        int numPrevTasks = 1;
        int generation = 0;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .build();
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Verify that only the new records are consumed
        SourceRecords newInserts = consumeRecordsByTopic(numRecords + 1);
        assertThat(newInserts.allRecordsInOrder().size()).isEqualTo(numRecords + 1); // allow one extra record as we start reading at the recorded offset
        assertNoRecordsToConsume();
        for (SourceRecord record : newInserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }
    }

    /**
     * Verifies that multi-task connectors function correctly using default config values.
     *
     * 1. Start non-multitask connector
     * 2. Allow connector to consume records and publish offset
     * 3. Start multi-task connector for the first time with negative previous generation
     * 4. Allow connector to consume records and publish offset
     * 5. Assert that connector consumes records starting from first connector's offset
     * @throws Exception
     */
    @Test
    public void shouldStartWithUnsetPrevConfigs() throws Exception {
        // Get new config without unnecessary fields set
        config = TestHelper.getConfiguration(mongo).edit()
                .with(MongoDbConnectorConfig.TOPIC_PREFIX, "mongo")
                .with(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, db)
                .build();

        // Start connector with multitask disabled
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        int numRecords = 100;
        populateDataCollection(numRecords);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);

        // Stop connector
        stopConnector();

        // Populate new records over 12s (default window size is 10s)
        populateDataCollectionOverTime(numRecords, 12000, 1000);

        // Start multitask connector
        int numTasks = 2;
        int generation = 0;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .build();
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Add new records and verify that the new records are consumed
        SourceRecords newInserts = consumeRecordsByTopic(numRecords + 1); // allow one extra record as we start reading at the recorded offset
        assertThat(newInserts.allRecordsInOrder().size()).isEqualTo(numRecords + 1);
        assertNoRecordsToConsume();
        for (SourceRecord record : newInserts.allRecordsInOrder()) {
            validateEventInWindow(record, 10, numTasks);
        }
    }

    /**
     * Verifies that multi-task connectors get offsets from the previous generation on scale up/down even if the previous
     * generation != generation - 1.
     *
     * 1. Start multi-task connector for the first time
     * 2. Allow connector to consume records and publish offset
     * 3. Start connector in two generations ahead with previous generation set to 0
     * 4. Allow connector to consume records and assert that it only consumes records created after previous connector's offset
     *
     * @throws Exception
     */
    @Test
    public void multiTaskConnectorShouldGetOffsetsFromPrevGenWithDifferenceGreaterThanOne() throws Exception {
        int numTasks = 2;
        int generation = 0;
        int numPrevTasks = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .build();

        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Insert records
        int numRecords = 100;
        populateDataCollectionOverTime(numRecords, 5000, 100);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);
        for (SourceRecord record : inserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }

        // Record time
        Instant firstConnectorStopTime = Instant.now();

        // Stop connector
        stopConnector();

        // Add new records
        populateDataCollectionOverTime(numRecords, 5000, 100);

        // Scale up
        numPrevTasks = numTasks;
        numTasks = 4;
        int prevGen = generation;
        generation = 2;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, prevGen)
                .build();
        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Verify that connector starts consuming records from previous generation's offset
        SourceRecords newInserts = consumeRecordsByTopic(numRecords * 2);
        assertNoRecordsToConsume();
        assertThat(newInserts.allRecordsInOrder().size()).isGreaterThan(numRecords);
        for (SourceRecord record : newInserts.allRecordsInOrder()) {
            long timestamp = getTimestamp(record);
            assertThat(timestamp).isGreaterThan(firstConnectorStopTime.toEpochMilli());
            validateEventInWindow(record, 1, numTasks);
        }
    }

    /**
     * Verifies that non-multitask connectors can use saved multi-task offsets on startup.
     *
     * 1. Start multi-task connector
     * 2. Allow connector to consume records and publish offset
     * 3. Restart connector with multi-task disabled
     * 4. Allow connector to consume records and assert that it only consumes records created after multi-task offset
     * 5. Restart connector
     * 6. Allow connector to consume records and assert that it only consumes records created after its own offset
     *
     * @throws Exception
     */
    @Test
    public void connectorShouldGetMultiTaskOffsetsWhenMultiTaskDisabled() throws Exception {
        int numTasks = 2;
        int generation = 0;
        int numPrevTasks = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .build();

        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Insert records
        int numRecords = 100;
        populateDataCollectionOverTime(numRecords, 5000, 100);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isEqualTo(numRecords);
        for (SourceRecord record : inserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }

        // Stop connector
        stopConnector();

        // Add new records
        populateDataCollection(numRecords);

        // Disable multitask
        config = config.edit()
                .with(MONGODB_MULTI_TASK_ENABLED, false)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, generation)
                .build();
        start(MongoDbConnector.class, config);

        // Verify that connector starts consuming records from previous generation's offset
        SourceRecords newInserts = consumeRecordsByTopic(numRecords * 2);
        assertNoRecordsToConsume();
        assertThat(newInserts.allRecordsInOrder().size()).isGreaterThan(numRecords);

        // Restart connector with new records
        stopConnector();
        populateDataCollection(numRecords);
        start(MongoDbConnector.class, config);

        // Verify that connector starts consuming records from its own previously recorded offset
        SourceRecords newestInserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(newestInserts.allRecordsInOrder().size()).isEqualTo(numRecords);
    }

    /**
     * Verifies that non-multitask connectors will use saved multi-task offsets on startup even when out-of-date single
     * task offsets exist.
     *
     * 1. Start non-multitask connector
     * 2. Allow connector to consume records and publish offset
     * 3. Start multi-task connector
     * 4. Allow connector to consume records and publish offset
     * 5. Restart connector with multi-task disabled
     * 6. Allow connector to consume records and assert that it only consumes records created after multi-task offset
     *
     * @throws Exception
     */
    @Test
    public void connectorShouldGetMultiTaskOffsetsWhenMultiTaskDisabledEvenIfStaleSingleTaskOffsetsExist() throws Exception {
        // Start connector with multitask disabled
        config = config.edit()
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, false)
                .build();
        start(MongoDbConnector.class, config);
        waitForStreamingRunning("mongodb", "mongo");

        int numRecords = 100;
        populateDataCollection(numRecords);

        // Consume records
        SourceRecords originalInserts = consumeRecordsByTopic(numRecords);
        assertNoRecordsToConsume();
        assertThat(originalInserts.allRecordsInOrder().size()).isEqualTo(numRecords);

        // Stop connector
        stopConnector();

        // Enable multitask
        int numTasks = 2;
        int generation = 0;
        int numPrevTasks = 1;
        config = config.edit()
                .with(TASKS_MAX, numTasks)
                .with(MONGODB_MULTI_TASK_GEN, generation)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numPrevTasks)
                .with(MongoDbConnectorConfig.MONGODB_MULTI_TASK_ENABLED, true)
                .build();

        start(MongoDbConnector.class, config);
        for (int task = 0; task < numTasks; ++task) {
            waitForStreamingRunning("mongodb", "mongo", task);
        }

        // Insert records
        populateDataCollectionOverTime(numRecords, 5000, 100);

        // Consume records
        SourceRecords inserts = consumeRecordsByTopic(numRecords * 2);
        assertNoRecordsToConsume();
        assertThat(inserts.allRecordsInOrder().size()).isGreaterThan(numRecords);
        for (SourceRecord record : inserts.allRecordsInOrder()) {
            validateEventInWindow(record, 1, numTasks);
        }

        // Stop connector
        stopConnector();

        // Add new records
        populateDataCollection(numRecords);

        // Disable multitask
        config = config.edit()
                .with(MONGODB_MULTI_TASK_ENABLED, false)
                .with(MONGODB_MULTI_TASK_PREV_TASKS, numTasks)
                .with(MONGODB_MULTI_TASK_PREV_GEN, generation)
                .build();
        start(MongoDbConnector.class, config);

        // Verify that connector starts consuming records from previous generation's offset
        SourceRecords newInserts = consumeRecordsByTopic(numRecords * 3);
        assertNoRecordsToConsume();
        assertThat(newInserts.allRecordsInOrder().size()).isGreaterThan(numRecords);
        assertThat(newInserts.allRecordsInOrder().size()).isLessThan(numRecords * 2);
    }

    private ArrayList<Document> populateDataCollection(int nRecords) {
        ArrayList<Document> docs = new ArrayList<>();
        for (int i = 0; i < nRecords; i++) {
            ObjectId objId = new ObjectId();
            Document obj = new Document("_id", objId);
            insertDocuments(db, col, obj);
            docs.add(obj);
        }

        return docs;
    }

    private long getTimestamp(SourceRecord record) {
        final Object recordValue = record.value();
        if (recordValue != null && recordValue instanceof Struct) {
            final Struct value = (Struct) recordValue;

            final long timestamp = value.getInt64(Envelope.FieldName.TIMESTAMP);
            return timestamp;
        }
        return 0;
    }

    private long getClusterTime(SourceRecord record) {
        final Object recordValue = record.value();
        if (recordValue != null && recordValue instanceof Struct) {
            final Struct value = (Struct) recordValue;
            Struct t = value.getStruct(Envelope.FieldName.SOURCE);
            final long clusterTime = t.getInt64(Envelope.FieldName.TIMESTAMP);
            return clusterTime;
        }

        return -1;
    }

    private void validateEventInWindow(SourceRecord record, int hopSize, int taskCount) {
        final int taskId = Integer.parseInt((String) record.sourcePartition().get("task_id"));
        final long clusterTime = getClusterTime(record);

        BsonTimestamp ts = new BsonTimestamp((int) (clusterTime / 1000), 0);
        MultiTaskOffsetHandler mth = new MultiTaskOffsetHandler(ts, hopSize, taskCount, taskId);
        if (mth.oplogStart.getTime() > ts.getTime() || mth.oplogStop.getTime() < ts.getTime()) {
            throw new RuntimeException("Event timestamp " + ts.getTime() + " is not within the expected window: " + mth.oplogStart + " - " + mth.oplogStop);
        }
        assertThat(ts.getTime()).isGreaterThanOrEqualTo(mth.oplogStart.getTime());
        assertThat(ts.getTime()).isLessThanOrEqualTo(mth.oplogStop.getTime());
    }

    private void populateDataCollectionOverTime(int numRecords, int ms, int intervalMs) throws InterruptedException {
        int numIntervals = ms / intervalMs;
        int recordsPerInterval = numRecords / numIntervals;
        int writtenRecords = numRecords % numIntervals;
        populateDataCollection(writtenRecords);
        while (writtenRecords < numRecords) {
            populateDataCollection(recordsPerInterval);
            writtenRecords += recordsPerInterval;
            Thread.sleep(intervalMs);
        }
    }
}
