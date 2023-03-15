/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.partitions;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.LIST_SEPARATOR;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;
import io.debezium.transforms.spi.QualifiedTableNameResolver;

/**
 * This SMT allow to use a specific table column to calculate the destination partition.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class ComputePartition<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputePartition.class);

    private Set<String> tableNames;
    private SmtManager<R> smtManager;
    private Map<String, Integer> numberOfPartitionsByTable;
    private Map<String, String> fieldNameByTable;
    private final ServiceLoader<QualifiedTableNameResolver> qualifiedTableNameResolverServiceLoader = ServiceLoader.load(QualifiedTableNameResolver.class);

    @Override
    public ConfigDef config() {

        ConfigDef config = new ConfigDef();
        // group does not manage validator definition. Validation will not work here.
        return Field.group(config, "partitions",
                PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD, FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD);
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD, FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD));

        fieldNameByTable = ComputePartitionConfigDefinition.parseMappings(config.getStrings(PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD, LIST_SEPARATOR));
        numberOfPartitionsByTable = ComputePartitionConfigDefinition.parseParititionMappings(config.getStrings(FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD, LIST_SEPARATOR));

        checkConfigurationConsistency();

        tableNames = fieldNameByTable.keySet();
    }

    private void checkConfigurationConsistency() {

        if (numberOfPartitionsByTable.size() != fieldNameByTable.size()) {
            throw new ComputePartitionException(String.format("Unable to validate config. %s and %s has different number of table defined",
                    FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF, FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF));
        }

        Set<String> intersection = new HashSet<>(numberOfPartitionsByTable.keySet());
        intersection.retainAll(fieldNameByTable.keySet());

        if (intersection.size() != numberOfPartitionsByTable.size()) {
            throw new ComputePartitionException(
                    String.format("Unable to validate config. %s and %s has different tables defined", FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF,
                            FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF));
        }

        if (numberOfPartitionsByTable.containsValue(0)) {
            throw new ConnectException(String.format("Unable to validate config. %s: partition number cannot be 0", FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF));
        }
    }

    @Override
    public R apply(R record) {

        LOGGER.trace("Starting ComputePartition SMT with conf: {} {} {}", tableNames, fieldNameByTable, numberOfPartitionsByTable);

        if (record.value() == null || !smtManager.isValidEnvelope(record)) {
            LOGGER.trace("Skipping tombstone or message without envelope");
            return record;
        }

        final Struct envelope = (Struct) record.value();
        final Schema envelopeSchema = record.valueSchema();

        QualifiedTableNameResolver qualifiedTableNameResolver = lookupQualifiedTableNameResolver(envelope);

        RecordParser recordParser = qualifiedTableNameResolver.createParser(envelopeSchema, envelope);

        try {
            final String qualifiedTableName = qualifiedTableNameResolver.resolve(recordParser);

            if (skipRecord(qualifiedTableName)) {
                return record;
            }

            Optional<Struct> data = extractPayload(recordParser);

            if (data.isEmpty()) {
                return record;
            }

            Object fieldValue = data.get().get(fieldNameByTable.get(qualifiedTableName));
            int partition = computePartition(fieldValue, qualifiedTableName);

            LOGGER.trace("Message {} will be sent to partition {}", envelope, partition);

            return record.newRecord(record.topic(), partition,
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    envelope,
                    record.timestamp(),
                    record.headers());
        }
        catch (Exception e) {
            LOGGER.error("Error occurred while processing message {}. Skipping SMT", envelope);
            throw new ConnectException(String.format("Unprocessable message %s", envelope), e);
        }
    }

    private QualifiedTableNameResolver lookupQualifiedTableNameResolver(Struct envelope) {

        String connectorType = envelope.getStruct(Envelope.FieldName.SOURCE).getString(AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY);
        return qualifiedTableNameResolverServiceLoader.findFirst()
                .orElseThrow(() -> new DataException("No usable QualifiedTableNameResolver for connector type \"" + connectorType + "\""));
    }

    private boolean skipRecord(String table) {

        if (!tableNames.contains(table)) {
            LOGGER.trace("Table {} is not configured. Skipping SMT", table);
            return true;
        }

        return false;
    }

    private int computePartition(Object fieldValue, String table) {
        // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a negative number
        // we use bitwise operation to remove the sign
        return (fieldValue.hashCode() & Integer.MAX_VALUE) % numberOfPartitionsByTable.get(table);
    }

    private Optional<Struct> extractPayload(RecordParser recordParser) {

        Envelope.Operation operation = Envelope.Operation.forCode(recordParser.op());

        if (operation == null) {
            throw new IllegalArgumentException("Unknown event operation: " + recordParser.op());
        }

        switch (operation) {
            case CREATE:
            case READ:
            case UPDATE:
                return Optional.of((Struct) recordParser.data().get(AFTER));
            case DELETE:
                return Optional.of((Struct) recordParser.data().get(BEFORE));
            case TRUNCATE:
            case MESSAGE:
                return Optional.empty();
            default:
                throw new IllegalArgumentException("Unable to get payload. Unmanaged event operation: " + operation);
        }
    }

    @Override
    public void close() {
    }
}
