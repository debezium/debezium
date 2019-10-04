/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;

public class ExtractNewRecordStateConfigDefinition {

    public static final String DEBEZIUM_OPERATION_HEADER_KEY = "__debezium-operation";
    public static final String DELETED_FIELD = "__deleted";
    public static final String METADATA_FIELD_PREFIX = "__";

    public static enum DeleteHandling implements EnumeratedValue {
        DROP("drop"),
        REWRITE("rewrite"),
        NONE("none");

        private final String value;

        private DeleteHandling(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static DeleteHandling parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (DeleteHandling option : DeleteHandling.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static DeleteHandling parse(String value, String defaultValue) {
            DeleteHandling mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field DROP_TOMBSTONES = Field.create("drop.tombstones")
            .withDisplayName("Drop tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Debezium by default generates a tombstone record to enable Kafka compaction after "
                    + "a delete record was generated. This record is usually filtered out to avoid duplicates "
                    + "as a delete record is converted to a tombstone record, too");

    public static final Field HANDLE_DELETES = Field.create("delete.handling.mode")
            .withDisplayName("Handle delete records")
            .withEnum(DeleteHandling.class, DeleteHandling.DROP)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How to handle delete records. Options are: "
                    + "none - records are passed,"
                    + "drop - records are removed (the default),"
                    + "rewrite - __deleted field is added to records.");

    public static final Field OPERATION_HEADER = Field.create("operation.header")
            .withDisplayName("Adds a message header representing the applied operation")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(false)
            .withDescription("Adds the operation {@link FieldName#OPERATION operation} as a header." +
                    "Its key is '" + ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY + "'");

    public static final Field ADD_SOURCE_FIELDS = Field.create("add.source.fields")
            .withDisplayName("Adds the specified fields from the 'source' field from the payload if they exist.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("")
            .withDescription("Adds each field listed from the 'source' element of the payload, prefixed with __ "
                    + "Example: 'version,connector' would add __version and __connector fields");
}
