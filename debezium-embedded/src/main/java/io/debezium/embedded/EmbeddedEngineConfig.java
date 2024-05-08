/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.time.Duration;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;

import io.debezium.config.Field;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.pipeline.ChangeEventSourceCoordinator;

/**
 * Common configuration options used in embedded implementations of {@link io.debezium.engine.DebeziumEngine}.
 */
public interface EmbeddedEngineConfig {

    /**
     * A required field for an embedded connector that specifies the unique name for the connector instance.
     */
    Field ENGINE_NAME = Field.create("name")
            .withDescription("Unique name for this connector instance.")
            .required();

    /**
     * A required field for an embedded connector that specifies the name of the normal Debezium connector's Java class.
     */
    Field CONNECTOR_CLASS = Field.create("connector.class")
            .withDescription("The Java class for the connector")
            .required();

    /**
     * The array of fields that are required by each connectors.
     */
    Field.Set CONNECTOR_FIELDS = Field.setOf(ENGINE_NAME, CONNECTOR_CLASS);

    /**
     * An optional field that specifies the name of the class that implements the {@link OffsetBackingStore} interface,
     * and that will be used to store offsets recorded by the connector.
     */
    Field OFFSET_STORAGE = Field.create("offset.storage")
            .withDescription("The Java class that implements the `OffsetBackingStore` "
                    + "interface, used to periodically store offsets so that, upon "
                    + "restart, the connector can resume where it last left off.")
            .withDefault(FileOffsetBackingStore.class.getName());

    /**
     * An optional field that specifies the file location for the {@link FileOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    Field OFFSET_STORAGE_FILE_FILENAME = Field.create(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG)
            .withDescription("The file where offsets are to be stored. Required when "
                    + "'offset.storage' is set to the " +
                    FileOffsetBackingStore.class.getName() + " class.")
            .withDefault("");

    /**
     * An optional field that specifies the topic name for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    Field OFFSET_STORAGE_KAFKA_TOPIC = Field.create(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG)
            .withDescription("The name of the Kafka topic where offsets are to be stored. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.")
            .withDefault("");

    /**
     * An optional field that specifies the number of partitions for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    Field OFFSET_STORAGE_KAFKA_PARTITIONS = Field.create(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG)
            .withType(ConfigDef.Type.INT)
            .withDescription("The number of partitions used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional field that specifies the replication factor for the {@link KafkaOffsetBackingStore}.
     *
     * @see #OFFSET_STORAGE
     */
    Field OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR = Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
            .withType(ConfigDef.Type.SHORT)
            .withDescription("Replication factor used when creating the offset storage topic. "
                    + "Required with other properties when 'offset.storage' is set to the "
                    + KafkaOffsetBackingStore.class.getName() + " class.");

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    Field OFFSET_FLUSH_INTERVAL_MS = Field.create("offset.flush.interval.ms")
            .withDescription("Interval at which to try committing offsets, given in milliseconds. Defaults to 1 minute (60,000 ms).")
            .withDefault(60000L)
            .withValidation(Field::isNonNegativeInteger);

    /**
     * An optional advanced field that specifies the maximum amount of time that the embedded connector should wait
     * for an offset commit to complete.
     */
    Field OFFSET_COMMIT_TIMEOUT_MS = Field.create("offset.flush.timeout.ms")
            .withDescription("Time to wait for records to flush and partition offset data to be"
                    + " committed to offset storage before cancelling the process and restoring the offset "
                    + "data to be committed in a future attempt, given in milliseconds. Defaults to 5 seconds (5000 ms).")
            .withDefault(5000L)
            .withValidation(Field::isPositiveInteger);

    Field OFFSET_COMMIT_POLICY = Field.create("offset.commit.policy")
            .withDescription("The fully-qualified class name of the commit policy type. This class must implement the interface "
                    + OffsetCommitPolicy.class.getName()
                    + ". The default is a periodic commit policy based upon time intervals.")
            .withDefault(OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class.getName())
            .withValidation(Field::isClassName);

    /**
     * A list of Predicates that can be assigned to transformations.
     */
    Field PREDICATES = Field.create("predicates")
            .withDisplayName("List of prefixes defining predicates.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional list of predicates that can be assigned to transformations. "
                    + "The predicates are defined using '<predicate.prefix>.type' config option and configured using options '<predicate.prefix>.<option>'");

    /**
     * A list of SMTs to be applied on the messages generated by the engine.
     */
    Field TRANSFORMS = Field.create("transforms")
            .withDisplayName("List of prefixes defining transformations.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Optional list of single message transformations applied on the messages. "
                    + "The transforms are defined using '<transform.prefix>.type' config option and configured using options '<transform.prefix>.<option>'");

    Field ERRORS_RETRY_DELAY_INITIAL_MS = Field.create("errors.retry.delay.initial.ms")
            .withDisplayName("Initial delay for retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(300)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Initial delay (in ms) for retries when encountering connection errors."
                    + " This value will be doubled upon every retry but won't exceed 'errors.retry.delay.max.ms'.");

    Field ERRORS_RETRY_DELAY_MAX_MS = Field.create("errors.retry.delay.max.ms")
            .withDisplayName("Max delay between retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(10000)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Max delay (in ms) between retries when encountering connection errors.");

    Field WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS = Field.create("debezium.embedded.shutdown.pause.before.interrupt.ms")
            .withDisplayName("Time to wait to engine completion before interrupt")
            .withType(ConfigDef.Type.LONG)
            .withDefault(Duration.ofMinutes(5).toMillis())
            .withValidation(Field::isPositiveInteger)
            .withDescription(String.format("How long we wait before forcefully stopping the connector thread when shutting down. " +
                    "Must be bigger than the time it takes two polling loops to finish ({} ms)", ChangeEventSourceCoordinator.SHUTDOWN_WAIT_TIMEOUT.toMillis() * 2));

    int DEFAULT_ERROR_MAX_RETRIES = -1;

    Field ERRORS_MAX_RETRIES = Field.create("errors.max.retries")
            .withDisplayName("The maximum number of retries")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DEFAULT_ERROR_MAX_RETRIES)
            .withValidation(Field::isInteger)
            .withDescription("The maximum number of retries on connection errors before failing (-1 = no limit, 0 = disabled, > 0 = num of retries).");

    /**
     * The array of all exposed fields.
     */
    Field.Set ALL_FIELDS = CONNECTOR_FIELDS.with(OFFSET_STORAGE, OFFSET_STORAGE_FILE_FILENAME,
            OFFSET_FLUSH_INTERVAL_MS, OFFSET_COMMIT_TIMEOUT_MS,
            ERRORS_MAX_RETRIES, ERRORS_RETRY_DELAY_INITIAL_MS, ERRORS_RETRY_DELAY_MAX_MS);
}
