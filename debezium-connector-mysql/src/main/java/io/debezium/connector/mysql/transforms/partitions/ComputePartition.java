/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.transforms.partitions;

import static io.debezium.connector.mysql.transforms.partitions.ComputePartitionConf.*;
import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.data.Envelope.FieldName.SOURCE;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;

public class ComputePartition<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputePartition.class);
    public static final String TABLE_FIELD_NAME = "table";

    private List<String> tableNames;
    private SmtManager<R> smtManager;
    private Map<String, Integer> numberOfPartitionsByTable;
    private Map<String, String> fieldNameByTable;

    @Override
    public R apply(R r) {

        LOGGER.debug("Starting ComputePartition STM with conf: {} {} {}", tableNames, fieldNameByTable, numberOfPartitionsByTable);

        if (r.value() == null || !smtManager.isValidEnvelope(r)) {
            LOGGER.debug("SMT skipped for invalid envelope or null value");
            return r;
        }

        final Struct envelope = (Struct) r.value();
        final String table = getTableName(envelope);

        if (skipRecord(table)) {
            return r;
        }

        try {
            Struct payload = extractPayload(envelope);

            Object fieldValue = payload.get(fieldNameByTable.get(table));
            int partition = computePartition(fieldValue, table);

            LOGGER.debug("Message {} will be sent to partition {}", envelope, partition);

            return r.newRecord(r.topic(), partition,
                    r.keySchema(),
                    r.key(),
                    r.valueSchema(),
                    envelope,
                    r.timestamp());
        }
        catch (Exception e) {
            LOGGER.error("Error occurred while processing message {}. Skipping SMT", envelope);
            return r;
        }
    }

    private boolean skipRecord(String table) {

        if (!tableNames.contains(table)) {
            LOGGER.debug("Table {} is not configured. Skipping STM", table);
        }
        if (!fieldNameByTable.containsKey(table)) {
            LOGGER.debug("No field name property defined for table {}. Skipping STM", table);
        }
        if (!numberOfPartitionsByTable.containsKey(table)) {
            LOGGER.debug("No number of partition property defined for table {}. Skipping STM", table);
        }

        return !tableNames.contains(table) || !fieldNameByTable.containsKey(table) || !numberOfPartitionsByTable.containsKey(table);
    }

    private String getTableName(Struct envelope) {

        Struct struct = (Struct) envelope.get(SOURCE);
        return struct.getString(TABLE_FIELD_NAME);
    }

    private int computePartition(Object fieldValue, String table) {

        return fieldValue.hashCode() % numberOfPartitionsByTable.get(table);
    }

    private Struct extractPayload(Struct envelope) {

        String operation = envelope.getString(OPERATION);

        Struct struct = (Struct) envelope.get(AFTER);
        if (Envelope.Operation.DELETE.code().equals(operation)) {
            struct = (Struct) envelope.get(BEFORE);
        }
        return struct;
    }

    @Override
    public ConfigDef config() {

        ConfigDef config = new ConfigDef();
        // group does not manage validator definition. Validation will not work here.
        return Field.group(config, "partitions",
                PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD, FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD, PARTITION_TABLE_LIST_FIELD);
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD));

        tableNames = config.getStrings(PARTITION_TABLE_LIST_FIELD, LIST_SEPARATOR);
        numberOfPartitionsByTable = ComputePartitionConf.parseIntMappings(config.getStrings(FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD, LIST_SEPARATOR));
        fieldNameByTable = ComputePartitionConf.parseMappings(config.getStrings(PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD, LIST_SEPARATOR));
    }

    @Override
    public void close() {

    }
}
