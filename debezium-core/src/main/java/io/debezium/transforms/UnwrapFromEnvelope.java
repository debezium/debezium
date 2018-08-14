/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.Map;

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

    private static final String ENVELOPE_SCHEMA_NAME_SUFFIX = ".Envelope";
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

    private static final Field HANDLE_DELETES = Field.create("delete.handling.mode")
            .withDisplayName("Handle delete records")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(true)
            .withDescription("How to handle delete records. Options are: "
                    + "none - records are passed,"
                    + "drop - records are removed,"
                    + "rewrite - __delete field is added to records.");

    private boolean dropTombstones;
    private String handleDeletes;
    private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();
    private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
    private final InsertField<R> removedDelegate = new InsertField.Value<R>();
    private final InsertField<R> updatedDelegate = new InsertField.Value<R>();

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final Field.Set configFields = Field.setOf(DROP_TOMBSTONES, HANDLE_DELETES);
        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        dropTombstones = config.getBoolean(DROP_TOMBSTONES);
        handleDeletes = config.getString(HANDLE_DELETES);

        Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", "before");
        beforeDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("field", "after");
        afterDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", "__deleted!");
        delegateConfig.put("static.value", "true");
        removedDelegate.configure(delegateConfig);

        delegateConfig = new HashMap<>();
        delegateConfig.put("static.field", "__deleted!");
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
                case "drop":
                    logger.trace("Delete message {} requested to be dropped", record.key());
                    return null;
                case "rewrite":
                    logger.trace("Delete message {} requested to be rewritten", record.key());
                    final R oldRecord = beforeDelegate.apply(record);
                    return removedDelegate.apply(oldRecord);
                default:
                    return newRecord;
            }
        } else {
            // Handling insert and update records
            switch (handleDeletes) {
                case "rewrite":
                    logger.trace("Delete message {} requested to be rewritten", record.key());
                    return updatedDelegate.apply(newRecord);
                default:
                    return newRecord;
            }
        }
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, DROP_TOMBSTONES, HANDLE_DELETES);
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
