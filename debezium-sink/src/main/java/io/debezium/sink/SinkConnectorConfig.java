/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.sink.filter.FieldFilterFactory.FieldNameFilter;
import io.debezium.sink.naming.CollectionNamingStrategy;
import io.debezium.sink.naming.DefaultCollectionNamingStrategy;

public interface SinkConnectorConfig {

    /**
     * Different modes that which primary keys are handled.
     */
    enum PrimaryKeyMode implements EnumeratedValue {
        /**
         * No keys are utilized, meaning tables will be created or managed without any primary keys.
         */
        NONE("none"),

        /**
         * The Kafka event coordinates are used as the primary key, which include the topic name,
         * the partition, and the offset associated with the event.
         */
        KAFKA("kafka"),

        /**
         * Fields from the record key are to be used, which can include struct-based keys or primitives.
         *
         * In the case of primitives, the {@code primary.key.fields} property must be supplied with a
         * single value, which controls what the primary key column name will be in the destination
         * table.
         *
         * In the case of a struct, the {@code primary.key.fields} property is optional and if specified
         * can specify a subset of the record's key fields to for the destination table's primary key.
         */
        RECORD_KEY("record_key"),

        /**
         * Fields from the event's record value are used. The {@code primary.key.fields} property should
         * be specified to identify the fields that will be the basis for the destination table's
         * primary key.
         */
        RECORD_VALUE("record_value"),

        /**
         * Fields from the event's record header are used. The {@code primary.key.fields} property should
         * be specified to identify the fields that will be the basis for the destination table's
         * primary key.
         */
        RECORD_HEADER("record_header");

        private final String mode;

        PrimaryKeyMode(String mode) {
            this.mode = mode;
        }

        public static PrimaryKeyMode parse(String value) {
            for (PrimaryKeyMode option : PrimaryKeyMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return PrimaryKeyMode.RECORD_KEY;
        }

        @Override
        public String getValue() {
            return mode;
        }
    }

    /**
     * Different modes for how keyed messages are batched.
     */
    enum KeyedMessageBatchMode implements EnumeratedValue {
        /**
         * All events are preserved and passed through without deduplication.
         * Batches are split at duplicate-key boundaries.
         */
        PASSTHROUGH("passthrough"),

        /**
         * Records with the same key are deduplicated within a batch (last-write-wins).
         */
        DEDUPLICATION("deduplication");

        private final String mode;

        KeyedMessageBatchMode(String mode) {
            this.mode = mode;
        }

        public static KeyedMessageBatchMode parse(String value) {
            for (KeyedMessageBatchMode option : KeyedMessageBatchMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return KeyedMessageBatchMode.DEDUPLICATION;
        }

        @Override
        public String getValue() {
            return mode;
        }
    }

    String COLLECTION_NAME_FORMAT = "collection.name.format";
    String DEPRECATED_TABLE_NAME_FORMAT = "table.name.format";
    Field COLLECTION_NAME_FORMAT_FIELD = Field.create(COLLECTION_NAME_FORMAT)
            .withDisplayName("A format string for the collection name")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault("${topic}")
            .withDescription("A format string for the table, which may contain '${topic}' as a placeholder for the original topic name.")
            .withDeprecatedAliases(DEPRECATED_TABLE_NAME_FORMAT);

    String COLLECTION_NAMING_STRATEGY = "collection.naming.strategy";
    String DEPRECATED_TABLE_NAMING_STRATEGY = "table.naming.strategy";
    Field COLLECTION_NAMING_STRATEGY_FIELD = Field.create(COLLECTION_NAMING_STRATEGY)
            .withDisplayName("CollectionNamingStrategy class")
            .withType(ConfigDef.Type.CLASS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultCollectionNamingStrategy.class.getName())
            .withDescription("Name of the strategy class that defines the collection name from the topic name. It implements the CollectionNamingStrategy interface.")
            .withDeprecatedAliases(DEPRECATED_TABLE_NAMING_STRATEGY);

    String FIELD_INCLUDE_LIST = "field.include.list";
    Field FIELD_INCLUDE_LIST_FIELD = Field.create(FIELD_INCLUDE_LIST)
            .withDisplayName("Include Fields")
            .withType(ConfigDef.Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of fields that "
                    + "should be included in change events. The field names must be delimited by the format <topic>:<field> ");

    String FIELD_EXCLUDE_LIST = "field.exclude.list";
    Field FIELD_EXCLUDE_LIST_FIELD = Field.create(FIELD_EXCLUDE_LIST)
            .withDisplayName("Exclude Fields")
            .withType(ConfigDef.Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of fields that "
                    + "should be excluded from change events. The field names must be delimited by the format <topic>:<field> ");

    String TRUNCATE_ENABLED = "truncate.enabled";
    Field TRUNCATE_ENABLED_FIELD = Field.create(TRUNCATE_ENABLED)
            .withDisplayName("Controls whether tables can be truncated by the connector")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to process debezium event `t` as truncate statement.");

    String DELETE_ENABLED = "delete.enabled";
    Field DELETE_ENABLED_FIELD = Field.create(DELETE_ENABLED)
            .withDisplayName("Controls whether records can be deleted by the connector")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to treat `null` record values as deletes. Requires primary.key.mode to be `record.key`.");

    String PRIMARY_KEY_MODE = "primary.key.mode";
    Field PRIMARY_KEY_MODE_FIELD = Field.create(PRIMARY_KEY_MODE)
            .withDisplayName("Primary key mode")
            .withEnum(PrimaryKeyMode.class, PrimaryKeyMode.NONE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The primary key mode.");

    String BATCH_SIZE = "batch.size";
    Field BATCH_SIZE_FIELD = Field.create(BATCH_SIZE)
            .withDisplayName("Specifies how many records to attempt to batch together into the destination table, when possible. " +
                    "You can also configure the connector’s underlying consumer’s max.poll.records using consumer.override.max.poll.records in the connector configuration.")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(500)
            .withDescription("Specifies how many records to attempt to batch together into the destination table, when possible. " +
                    "You can also configure the connector’s underlying consumer’s max.poll.records using consumer.override.max.poll.records in the connector configuration.");

    String KEYED_MESSAGE_BATCH_MODE = "keyed.message.batch.mode";
    Field KEYED_MESSAGE_BATCH_MODE_FIELD = Field.create(KEYED_MESSAGE_BATCH_MODE)
            .withDisplayName("Keyed message batch mode")
            .withEnum(KeyedMessageBatchMode.class, KeyedMessageBatchMode.DEDUPLICATION)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 5))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Controls how keyed records are batched. "
                    + "'deduplication' (default) deduplicates records by key within a batch (last-write-wins). "
                    + "'passthrough' preserves all events and splits batches at duplicate-key boundaries.");

    String PRIMARY_KEY_FIELDS = "primary.key.fields";
    Field PRIMARY_KEY_FIELDS_FIELD = Field.create(PRIMARY_KEY_FIELDS)
            .withDisplayName("Comma-separated list of primary key field names")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR))
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("A comma-separated list of primary key field names. " +
                    "This is interpreted differently depending on " + PRIMARY_KEY_MODE + ".");

    String CLOUDEVENTS_SCHEMA_NAME_PATTERN = "cloud.events.schema.name.pattern";
    Field CLOUDEVENTS_SCHEMA_NAME_PATTERN_FIELD = Field.create(CLOUDEVENTS_SCHEMA_NAME_PATTERN)
            .withDisplayName("CloudEvents messages schema name pattern")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(".*CloudEvents\\.Envelope$")
            .withDescription("Regular expression pattern to identify CloudEvents messages by matching the schema name with this pattern.");

    String ENABLE_SHARED_CHANGE_EVENT_SINK = "enable.sces";
    Field ENABLE_SHARED_CHANGE_EVENT_SINK_FIELD = Field.createInternal(ENABLE_SHARED_CHANGE_EVENT_SINK)
            .withDisplayName("Specifies whether to enable the new shared sink connector framework (sces) logic.")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 0))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(false)
            .withDescription("Enables usage of the new shared sink connector framework logic.");

    /**
     * Returns the collection name format.
     */
    String getCollectionNameFormat();

    /**
     * Returns the keyed message batch mode.
     * Either 'deduplication' (default) or 'passthrough'.
     * @see KeyedMessageBatchMode
     */
    KeyedMessageBatchMode getKeyedMessageBatchMode();

    /**
     * Returns the primary key mode.
     * Either 'none' (default) or 'record-key' or 'record-key-and-column' or 'column'.
     * @see PrimaryKeyMode
     */
    PrimaryKeyMode getPrimaryKeyMode();

    /**
     * Returns the primary key fields.
     */
    Set<String> getPrimaryKeyFields();

    /**
     * Returns true if truncate events handling is enabled.
     */
    boolean isTruncateEnabled();

    /**
     * Returns true if delete events handling is enabled.
     */
    boolean isDeleteEnabled();

    /**
     * Returns the timezone to use for date and time values when sending data to the datastore.
     */
    String useTimeZone();

    /**
     * Returns the batch size.
     */
    int getBatchSize();

    /**
     * Returns the collection naming strategy.
     */
    CollectionNamingStrategy getCollectionNamingStrategy();

    /**
     * Returns the field include/exclude filter.
     */
    FieldNameFilter fieldFilter();

    /**
     * Returns the CloudEvents schema name pattern which is used to identify CloudEvents messages.
     */
    String cloudEventsSchemaNamePattern();

}
