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

    public static final String DEBEZIUM_OPERATION_HEADER_KEY = "__op";
    public static final String DELETED_FIELD = "__deleted";
    public static final String METADATA_FIELD_PREFIX = "__";

    public enum DeleteTombstoneHandling implements EnumeratedValue {
        DROP("drop"),
        TOMBSTONE("tombstone"),
        REWRITE("rewrite"),
        REWRITE_WITH_TOMBSTONE("rewrite-with-tombstone"),
        DELETE_TO_TOMBSTONE("delete-to-tombstone");

        private final String value;

        DeleteTombstoneHandling(String value) {
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
        public static DeleteTombstoneHandling parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (DeleteTombstoneHandling option : DeleteTombstoneHandling.values()) {
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
        public static DeleteTombstoneHandling parse(String value, String defaultValue) {
            DeleteTombstoneHandling mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field HANDLE_TOMBSTONE_DELETES = Field.create("delete.tombstone.handling.mode")
            .withDisplayName("Handle delete and tombstone records")
            .withEnum(DeleteTombstoneHandling.class)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(DeleteTombstoneHandling.TOMBSTONE.getValue())
            .withDescription("How to handle delete records. Options are: "
                    + "drop - Remove the delete event and tombstone from the stream."
                    + "tombstone (default) - For each delete event, leave only a tombstone in the stream."
                    + "rewrite - Remove tombstone from the record, and add a `__deleted` field with the value `true`."
                    + "rewrite-with-tombstone - Retain tombstone in record and add a `__deleted` field with the value `true`."
                    + "rewrite-deletes - Convert delete events to tombstone events and drop tombstone events.");

    public static final Field ROUTE_BY_FIELD = Field.create("route.by.field")
            .withDisplayName("Route by field name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The column which determines how the events will be routed, the value will replace the topic name.")
            .withDefault("");

    public static final Field ADD_FIELDS_PREFIX = Field.create("add.fields.prefix")
            .withDisplayName("Field prefix to be added to each field.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(METADATA_FIELD_PREFIX)
            .withDescription("Adds this prefix to each field listed.");

    public static final Field ADD_FIELDS = Field.create("add.fields")
            .withDisplayName("Adds the specified field(s) to the message if they exist.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("")
            .withDescription("Adds each field listed, prefixed with __ (or __<struct>_ if the struct is specified). "
                    + "Example: 'version,connector,source.ts_ms' would add __version, __connector and __source_ts_ms fields. "
                    + "Optionally one can also map new field name like version:VERSION,connector:CONNECTOR,source.ts_ms:EVENT_TIMESTAMP."
                    + "Please note that the new field name is case-sensitive.");

    public static final Field ADD_HEADERS_PREFIX = Field.create("add.headers.prefix")
            .withDisplayName("Header prefix to be added to each header.")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(METADATA_FIELD_PREFIX)
            .withDescription("Adds this prefix listed to each header.");

    public static final Field ADD_HEADERS = Field.create("add.headers")
            .withDisplayName("Adds the specified fields to the header if they exist.")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault("")
            .withDescription("Adds each field listed to the header,  __ (or __<struct>_ if the struct is specified). "
                    + "Example: 'version,connector,source.ts_ms' would add __version, __connector and __source_ts_ms fields. "
                    + "Optionally one can also map new field name like version:VERSION,connector:CONNECTOR,source.ts_ms:EVENT_TIMESTAMP."
                    + "Please note that the new field name is case-sensitive.");

    public static final Field REPLACE_NULL_WITH_DEFAULT = Field.create("replace.null.with.default")
            .withDisplayName("replace null field value with default")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .optional()
            .withDescription("When this option is enabled, null field values are replaced by source-defined defaults when rewriting the record.");

    public static final Field.Set CONFIG_FIELDS = Field.setOf(
            HANDLE_TOMBSTONE_DELETES, ROUTE_BY_FIELD, ADD_FIELDS_PREFIX, ADD_FIELDS, ADD_HEADERS_PREFIX, ADD_HEADERS);
}
