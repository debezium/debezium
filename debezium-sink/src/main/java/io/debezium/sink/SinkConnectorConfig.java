/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink;

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

    String COLLECTION_NAME_FORMAT = "collection.name.format";
    String DEPRECATED_TABLE_NAME_FORMAT = "table.name.format";
    Field COLLECTION_NAME_FORMAT_FIELD = Field.create(COLLECTION_NAME_FORMAT)
            .withDisplayName("A format string for the collection name")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 3))
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
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DefaultCollectionNamingStrategy.class.getName())
            .withDescription("Name of the strategy class that defines the collection name from the topic name. It implements the CollectionNamingStrategy interface.")
            .withDeprecatedAliases(DEPRECATED_TABLE_NAMING_STRATEGY);

    String FIELD_INCLUDE_LIST = "field.include.list";
    Field FIELD_INCLUDE_LIST_FIELD = Field.create(FIELD_INCLUDE_LIST)
            .withDisplayName("Include Fields")
            .withType(ConfigDef.Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 1))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of fields that "
                    + "should be included in change events. The field names must be delimited by the format <topic>:<field> ");

    String FIELD_EXCLUDE_LIST = "field.exclude.list";
    Field FIELD_EXCLUDE_LIST_FIELD = Field.create(FIELD_EXCLUDE_LIST)
            .withDisplayName("Exclude Fields")
            .withType(ConfigDef.Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.FILTERS, 2))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription("A comma-separated list of regular expressions matching fully-qualified names of fields that "
                    + "should be excluded from change events. The field names must be delimited by the format <topic>:<field> ");

    String TRUNCATE_ENABLED = "truncate.enabled";
    Field TRUNCATE_ENABLED_FIELD = Field.create(TRUNCATE_ENABLED)
            .withDisplayName("Controls whether tables can be truncated by the connector")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to process debezium event `t` as truncate statement.");

    String DELETE_ENABLED = "delete.enabled";
    Field DELETE_ENABLED_FIELD = Field.create(DELETE_ENABLED)
            .withDisplayName("Controls whether records can be deleted by the connector")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 2))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to treat `null` record values as deletes. Requires primary.key.mode to be `record.key`.");

    String PRIMARY_KEY_MODE = "primary.key.mode";
    Field PRIMARY_KEY_MODE_FIELD = Field.create(PRIMARY_KEY_MODE)
            .withDisplayName("Primary key mode")
            .withEnum(PrimaryKeyMode.class, PrimaryKeyMode.NONE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The primary key mode.");

    String DEFAULT_TIME_ZONE = "UTC";
    String USE_TIME_ZONE = "use.time.zone";
    String DEPRECATED_DATABASE_TIME_ZONE = "database.time_zone";
    Field USE_TIME_ZONE_FIELD = Field.create(USE_TIME_ZONE)
            .withDisplayName("The timezone used when inserting temporal values.")
            .withDefault(DEFAULT_TIME_ZONE)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 6))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("The timezone used when inserting temporal values. Defaults to UTC.")
            .withDeprecatedAliases(DEPRECATED_DATABASE_TIME_ZONE);

    String BATCH_SIZE = "batch.size";
    Field BATCH_SIZE_FIELD = Field.create(BATCH_SIZE)
            .withDisplayName("Specifies how many records to attempt to batch together into the destination table, when possible. " +
                    "You can also configure the connector’s underlying consumer’s max.poll.records using consumer.override.max.poll.records in the connector configuration.")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 4))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(500)
            .withDescription("Specifies how many records to attempt to batch together into the destination table, when possible. " +
                    "You can also configure the connector’s underlying consumer’s max.poll.records using consumer.override.max.poll.records in the connector configuration.");

    String CLOUDEVENTS_SCHEMA_NAME_PATTERN = "cloud.events.schema.name.pattern";
    Field CLOUDEVENTS_SCHEMA_NAME_PATTERN_FIELD = Field.create(CLOUDEVENTS_SCHEMA_NAME_PATTERN)
            .withDisplayName("CloudEvents messages schema name pattern")
            .withType(ConfigDef.Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 8))
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(".*CloudEvents\\.Envelope$")
            .withDescription("Regular expression pattern to identify CloudEvents messages by this schema name pattern.");

    String getCollectionNameFormat();

    PrimaryKeyMode getPrimaryKeyMode();

    boolean isTruncateEnabled();

    boolean isDeleteEnabled();

    String useTimeZone();

    int getBatchSize();

    CollectionNamingStrategy getCollectionNamingStrategy();

    FieldNameFilter getFieldFilter();

    String cloudEventsSchemaNamePattern();

}
