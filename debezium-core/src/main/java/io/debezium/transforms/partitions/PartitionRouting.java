/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.partitions;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;
import io.debezium.util.MurmurHash3;

/**
 * This SMT allow to use payload fields to calculate the destination partition.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class PartitionRouting<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionRouting.class);
    private static final MurmurHash3 MURMUR_HASH_3 = MurmurHash3.getInstance();
    public static final String NESTING_SEPARATOR = "\\.";
    public static final String CHANGE_SPECIAL_FIELD = "change";
    public static final String FIELD_PAYLOAD_FIELD_CONF = "partition.payload.fields";
    public static final String FIELD_TOPIC_PARTITION_NUM_CONF = "partition.topic.num";
    public static final String FIELD_HASH_FUNCTION = "partition.hash.function";

    public enum HashFunction implements EnumeratedValue {
        /**
         * Hash function to be used when computing hash of the fields of the record.
         */

        JAVA("java", Object::hashCode),
        MURMUR("murmur", MurmurHash3.getInstance()::hash);

        private final String name;
        private final Function<Object, Integer> hash;

        HashFunction(String value, Function<Object, Integer> hash) {
            this.name = value;
            this.hash = hash;
        }

        @Override
        public String getValue() {
            return name;
        }

        public Function<Object, Integer> getHash() {
            return hash;
        }

        public static HashFunction parse(String value) {
            if (value == null) {
                return JAVA;
            }
            value = value.trim().toLowerCase();
            for (HashFunction option : HashFunction.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return JAVA;
        }
    }

    static final Field PARTITION_PAYLOAD_FIELDS_FIELD = Field.create(FIELD_PAYLOAD_FIELD_CONF)
            .withDisplayName("List of payload fields to use for compute partition.")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(
                    Field::notContainEmptyElements)
            .withDescription("Payload fields to use to calculate the partition. Supports Struct nesting using dot notation." +
                    "To access fields related to data collections, you can use: after, before or change, " +
                    "where 'change' is a special field that will automatically choose, based on operation, the 'after' or 'before'. " +
                    "If a field not exist for the current record it will simply not used" +
                    "e.g. after.name,source.table,change.name")
            .required();

    static final Field TOPIC_PARTITION_NUM_FIELD = Field.create(FIELD_TOPIC_PARTITION_NUM_CONF)
            .withDisplayName("Number of partition configured for topic")
            .withType(ConfigDef.Type.INT)
            .withValidation(Field::isPositiveInteger)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Number of partition for the topic on which this SMT act. Use TopicNameMatches predicate to filter records by topic")
            .required();

    static final Field HASH_FUNCTION_FIELD = Field.create(FIELD_HASH_FUNCTION)
            .withDisplayName("Hash function")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Hash function to be used when computing hash of the fields which would determine number of the destination partition.")
            .withDefault("java")
            .optional();

    private SmtManager<R> smtManager;
    private List<String> payloadFields;
    private int partitionNumber;
    private HashFunction hashFc;

    @Override
    public ConfigDef config() {

        ConfigDef config = new ConfigDef();
        // group does not manage validator definition. Validation will not work here.
        return Field.group(config, "partitions",
                PARTITION_PAYLOAD_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD, HASH_FUNCTION_FIELD);
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);

        smtManager = new SmtManager<>(config);

        smtManager.validate(config, Field.setOf(PARTITION_PAYLOAD_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD));

        payloadFields = config.getList(PARTITION_PAYLOAD_FIELDS_FIELD);
        partitionNumber = config.getInteger(TOPIC_PARTITION_NUM_FIELD);
        hashFc = HashFunction.parse(config.getString(HASH_FUNCTION_FIELD));
    }

    @Override
    public R apply(R originalRecord) {

        LOGGER.trace("Starting PartitionRouting SMT with conf: {} {}", payloadFields, partitionNumber);

        if (originalRecord.value() == null || !smtManager.isValidEnvelope(originalRecord)) {
            LOGGER.trace("Skipping tombstone or message without envelope");
            return originalRecord;
        }

        final Struct envelope = (Struct) originalRecord.value();
        try {

            if (SmtManager.isGenericOrTruncateMessage((SourceRecord) originalRecord)) {
                return originalRecord;
            }

            List<Object> fieldsValue = payloadFields.stream()
                    .map(fieldName -> toValue(fieldName, envelope))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            if (fieldsValue.isEmpty()) {
                LOGGER.trace("None of the configured fields found on record {}. Skipping it.", envelope);
                return originalRecord;
            }

            int partition = computePartition(partitionNumber, fieldsValue);

            return buildNewRecord(originalRecord, envelope, partition);

        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unprocessable message %s", envelope), e);
        }
    }

    private Optional<Object> toValue(String fieldName, Struct envelope) {

        try {
            String[] subFields = Arrays.stream(fieldName.split(NESTING_SEPARATOR)).map(String::trim).toArray(String[]::new);

            if (subFields.length == 1) {
                return Optional.ofNullable(envelope.get(subFields[0]));
            }

            Struct lastStruct = getLastStruct(envelope, subFields);

            return Optional.ofNullable(lastStruct.get(subFields[subFields.length - 1]));
        }
        catch (DataException e) {
            LOGGER.trace("Field {} not found on payload {}. It will not be considered", fieldName, envelope);
            return Optional.empty();
        }

    }

    private static Struct getLastStruct(Struct envelope, String[] subFields) {

        Struct currectStruct = envelope;
        for (int i = 0; i < subFields.length - 1; i++) {

            String fieldName = getFieldName(envelope, subFields, i);
            currectStruct = currectStruct.getStruct(fieldName);
        }
        return currectStruct;
    }

    private static String getFieldName(Struct envelope, String[] subFields, int i) {

        String fieldName = subFields[i];
        if (CHANGE_SPECIAL_FIELD.equals(subFields[i])) {
            Envelope.Operation operation = Envelope.Operation.forCode(envelope.getString(OPERATION));
            fieldName = Envelope.Operation.DELETE.equals(operation) ? BEFORE : AFTER;
        }
        return fieldName;
    }

    private R buildNewRecord(R originalRecord, Struct envelope, int partition) {
        LOGGER.trace("Message {} will be sent to partition {}", envelope, partition);

        return originalRecord.newRecord(originalRecord.topic(), partition,
                originalRecord.keySchema(),
                originalRecord.key(),
                originalRecord.valueSchema(),
                envelope,
                originalRecord.timestamp(),
                originalRecord.headers());
    }

    protected int computePartition(Integer partitionNumber, List<Object> values) {
        int totalHashCode = values.stream().map(hashFc.getHash()).reduce(0, Integer::sum);
        // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a negative number
        // we use bitwise operation to remove the sign
        int normalizedHash = totalHashCode & Integer.MAX_VALUE;
        if (normalizedHash == Integer.MAX_VALUE) {
            normalizedHash = 0;
        }
        return normalizedHash % partitionNumber;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
