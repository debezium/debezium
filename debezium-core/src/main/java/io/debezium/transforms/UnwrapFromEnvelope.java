/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.Map;

import io.debezium.config.EnumeratedValue;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Debezium generates CDC (<code>Envelope</code>) records that are struct of values containing values
 * <code>before</code> and <code>after change</code>. Sink connectors usually are not able to work
 * with a complex structure so a user use this SMT to extract <code>after</code> value and send it down
 * unwrapped in <code>Envelope</code>.
 * <p>
 * The functionality is similar to <code>ExtractField</code> SMT but has a special semantics for handling
 * delete events; when delete event is emitted by database then Debezium emits two messages: a delete
 * message and a tombstone message that serves as a signal to Kafka compaction process.
 * <p>
 * The SMT by default drops the tombstone message created by Debezium and converts the delete message into
 * a tombstone message that can be dropped, too, if required.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Jiri Pechanec
 */
public class UnwrapFromEnvelope<R extends ConnectRecord<R>> implements Transformation<R> {

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
            if (value == null) return null;
            value = value.trim();
            for (DeleteHandling option : DeleteHandling.values()) {
                if (option.getValue().equalsIgnoreCase(value)) return option;
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
            if (mode == null && defaultValue != null) mode = parse(defaultValue);
            return mode;
        }
    }

    private static final String ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope";
    private static final String DELETED_FIELD = "__deleted!";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final Field DROP_TOMBSTONES = Field.create("drop.tombstones")
            .withDisplayName("Drop tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Debezium by default generates a tombstone record to enable Kafka compaction after "
                    + "a delete record was generated. This record is usually filtered out to avoid duplicates "
                    + "as a delete record is converted to a tombstone record, too");

    private static final Field DROP_DELETES = Field.create("drop.deletes")
            .withDisplayName("Drop outgoing tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Drop delete records converted to tombstones records if a processing connector "
                    + "cannot process them or a compaction is undesirable.");

    private static final Field HANDLE_DELETES = Field.create("delete.handling.mode")
            .withDisplayName("Handle delete records")
            .withEnum(DeleteHandling.class, DeleteHandling.DROP)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("How to handle delete records. Options are: "
                    + "none - records are passed,"
                    + "drop - records are removed,"
                    + "rewrite - __deleted field is added to records.");

    private boolean dropTombstones;
    private boolean dropDeletes;
    private DeleteHandling handleDeletes;
    private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();
    private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
    private final InsertField<R> removedDelegate = new InsertField.Value<R>();
    private final InsertField<R> updatedDelegate = new InsertField.Value<R>();

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final Field.Set configFields = Field.setOf(DROP_TOMBSTONES, DROP_DELETES, HANDLE_DELETES);
        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        dropTombstones = config.getBoolean(DROP_TOMBSTONES);
        handleDeletes = DeleteHandling.parse(config.getString(HANDLE_DELETES));
        if (config.hasKey(DROP_DELETES.name())) {
            logger.warn(DROP_DELETES.name() + " option is deprecated. Please use " + HANDLE_DELETES.name());
            dropDeletes = config.getBoolean(DROP_DELETES);
            if (dropDeletes) {
                handleDeletes = DeleteHandling.DROP;
            } else {
                handleDeletes = DeleteHandling.NONE;
            }
        }

        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", "before");
        beforeDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("field", "after");
        afterDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", DELETED_FIELD);
        delegateConfig.put("static.value", "true");
        removedDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", DELETED_FIELD);
        delegateConfig.put("static.value", "false");
        updatedDelegate.configure(delegateConfig);
    }

    @Override
    public R apply(final R record) {
        if (record.value() == null) {
            if (dropTombstones) {
                logger.trace("Tombstone {} arrived and requested to be dropped", record.key());
                return null;
            }
            return record;
        }
        if (record.valueSchema() == null ||
                record.valueSchema().name() == null ||
                !record.valueSchema().name().endsWith(ENVELOPE_SCHEMA_NAME_SUFFIX)) {
            logger.warn("Expected Envelope for transformation, passing it unchanged");
            return record;
        }
        final R newRecord = afterDelegate.apply(record);
        if (newRecord.value() == null) {
            // Handling delete records
            switch (handleDeletes) {
                case DROP:
                    logger.trace("Delete message {} requested to be dropped", record.key());
                    return null;
                case REWRITE:
                    logger.trace("Delete message {} requested to be rewritten", record.key());
                    final R oldRecord = beforeDelegate.apply(record);
                    return removedDelegate.apply(oldRecord);
                default:
                    return newRecord;
            }
        } else {
            // Handling insert and update records
            switch (handleDeletes) {
                case REWRITE:
                    logger.trace("Insert/update message {} requested to be rewritten", record.key());
                    return updatedDelegate.apply(newRecord);
                default:
                    return newRecord;
            }
        }
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, DROP_TOMBSTONES, DROP_DELETES, HANDLE_DELETES);
        return config;
    }

    @Override
    public void close() {
        beforeDelegate.close();
        afterDelegate.close();
        removedDelegate.close();
        updatedDelegate.close();
    }

}
