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

    private static final Field DROP_DELETES = Field.create("drop.deletes")
            .withDisplayName("Drop outgoing tombstones")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDefault(true)
            .withDescription("Drop delete records converted to tombstones records if a processing connector "
                    + "cannot process them or a compaction is undesirable.");

    private boolean dropTombstones;
    private boolean dropDeletes;
    private final ExtractField<R> delegate = new ExtractField.Value<R>();

    @Override
    public void configure(final Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        final Field.Set configFields = Field.setOf(DROP_TOMBSTONES, DROP_DELETES);
        if (!config.validateAndRecord(configFields, logger::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        dropTombstones = config.getBoolean(DROP_TOMBSTONES);
        dropDeletes = config.getBoolean(DROP_DELETES);

        final Map<String, String> delegateConfig = new HashMap<>();
        delegateConfig.put("field", "after");
        delegate.configure(delegateConfig);
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
        final R newRecord = delegate.apply(record);
        if (newRecord.value() == null && dropDeletes) {
            logger.trace("Delete message {} requested to be dropped", record.key());
            return null;
        }
        return newRecord;
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, DROP_TOMBSTONES, DROP_DELETES);
        return config;
    }

    @Override
    public void close() {
        delegate.close();
    }

}
