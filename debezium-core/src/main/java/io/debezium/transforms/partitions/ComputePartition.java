/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.partitions;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.data.Envelope.FieldName.SOURCE;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.LIST_SEPARATOR;
import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;

/**
 * This SMT allow to use a specific table column to calculate the destination partition.
 *
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class ComputePartition<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputePartition.class);
    public static final String SCHEMA_FIELD_NAME = "schema";
    public static final String SQLSERVER_CONNECTOR = "sqlserver";
    public static final String KEYSPACE_FIELD_NAME = "keyspace";
    public static final String TABLE_FIELD_NAME = "table";
    public static final String COLLECTION_FIELD_NAME = "collection";
    public static final String CONNECTOR_FIELD_NAME = "connector";
    public static final String MONGODB_CONNECTOR = "mongodb";
    public static final String DB_FIELD_NAME = "db";
    public static final String MYSQL_CONNECTOR = "mysql";
    public static final String POSTGRES_CONNECTOR = "postgres";
    public static final String ORACLE_CONNECTOR = "oracle";
    public static final String DB2_CONNECTOR = "db2";
    public static final String CASSANDRA_CONNECTOR = "cassandra";
    public static final String VITESS_CONNECTOR = "vitess";

    private Set<String> tableNames;
    private SmtManager<R> smtManager;
    private Map<String, Integer> numberOfPartitionsByTable;
    private Map<String, String> fieldNameByTable;

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
        numberOfPartitionsByTable = ComputePartitionConfigDefinition.parseIntMappings(config.getStrings(FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD, LIST_SEPARATOR));

        checkConfigurationConsistency();

        tableNames = fieldNameByTable.keySet();
    }

    private void checkConfigurationConsistency() {

        if (numberOfPartitionsByTable.size() != fieldNameByTable.size()) {
            throw new ConnectException(String.format("Unable to validate config. %s and %s has different number of table defined",
                    FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF, FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF));
        }

        Set<String> intersection = new HashSet<>(numberOfPartitionsByTable.keySet());
        intersection.retainAll(fieldNameByTable.keySet());

        if (intersection.size() != numberOfPartitionsByTable.size()) {
            throw new ConnectException(String.format("Unable to validate config. %s and %s has different tables defined", FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF,
                    FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF));
        }

        if (numberOfPartitionsByTable.containsValue(0)) {
            throw new ConnectException(String.format("Unable to validate config. %s: partition number cannot be 0", FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF));
        }
    }

    @Override
    public R apply(R r) {

        LOGGER.trace("Starting ComputePartition SMT with conf: {} {} {}", tableNames, fieldNameByTable, numberOfPartitionsByTable);

        if (r.value() == null || !smtManager.isValidEnvelope(r)) {
            LOGGER.trace("Skipping tombstone or message without envelope");
            return r;
        }

        final Struct envelope = (Struct) r.value();
        try {
            final String table = getTableName(envelope);

            if (skipRecord(table)) {
                return r;
            }

            Optional<Struct> payload = extractPayload(envelope);

            if (payload.isEmpty()) {
                return r;
            }

            Object fieldValue = payload.get().get(fieldNameByTable.get(table));
            int partition = computePartition(fieldValue, table);

            LOGGER.trace("Message {} will be sent to partition {}", envelope, partition);

            return r.newRecord(r.topic(), partition,
                    r.keySchema(),
                    r.key(),
                    r.valueSchema(),
                    envelope,
                    r.timestamp(),
                    r.headers());
        }
        catch (Exception e) {
            LOGGER.error("Error occurred while processing message {}. Skipping SMT", envelope);
            throw new ConnectException(String.format("Unprocessable message %s", envelope), e);
        }
    }

    private boolean skipRecord(String table) {

        if (!tableNames.contains(table)) {
            LOGGER.trace("Table {} is not configured. Skipping SMT", table);
            return true;
        }

        return false;
    }

    private String getTableName(Struct envelope) {

        Struct struct = (Struct) envelope.get(SOURCE);
        String connector = struct.getString(CONNECTOR_FIELD_NAME);

        String tablePrefix;
        String dataCollection;
        switch (connector) {
            case MONGODB_CONNECTOR:
                dataCollection = struct.getString(COLLECTION_FIELD_NAME);
                tablePrefix = struct.getString(DB_FIELD_NAME);
                break;
            case MYSQL_CONNECTOR:
                dataCollection = struct.getString(TABLE_FIELD_NAME);
                tablePrefix = struct.getString(DB_FIELD_NAME);
                break;
            case POSTGRES_CONNECTOR:
            case ORACLE_CONNECTOR:
            case SQLSERVER_CONNECTOR:
            case DB2_CONNECTOR:
                dataCollection = struct.getString(TABLE_FIELD_NAME);
                tablePrefix = struct.getString(SCHEMA_FIELD_NAME);
                break;
            case CASSANDRA_CONNECTOR:
            case VITESS_CONNECTOR:
                dataCollection = struct.getString(TABLE_FIELD_NAME);
                tablePrefix = struct.getString(KEYSPACE_FIELD_NAME);
                break;
            default:
                throw new IllegalArgumentException("Unmanaged connector: " + connector);
        }

        return String.format("%s.%s", tablePrefix, dataCollection);
    }

    private int computePartition(Object fieldValue, String table) {
        // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a negative number
        // we use bitwise operation to remove the sign
        return (fieldValue.hashCode() & Integer.MAX_VALUE) % numberOfPartitionsByTable.get(table);
    }

    private Optional<Struct> extractPayload(Struct envelope) {

        Envelope.Operation operation = Envelope.Operation.forCode(envelope.getString(OPERATION));

        if (operation == null) {
            throw new IllegalArgumentException("Unknown event operation: " + envelope.getString(OPERATION));
        }

        switch (operation) {
            case CREATE:
            case READ:
            case UPDATE:
                return Optional.of((Struct) envelope.get(AFTER));
            case DELETE:
                return Optional.of((Struct) envelope.get(BEFORE));
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
