/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms.partitions;

import static io.debezium.data.Envelope.FieldName.AFTER;
import static io.debezium.data.Envelope.FieldName.BEFORE;
import static io.debezium.data.Envelope.FieldName.OPERATION;
import static io.debezium.data.Envelope.Operation.MESSAGE;
import static io.debezium.data.Envelope.Operation.TRUNCATE;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.transforms.SmtManager;

/**
 * This SMT allow to use payload fields to calculate the destination partition.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Mario Fiore Vitale
 */
public class PartitionRouting<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionRouting.class);
    public static final String NESTING_SEPARATOR = "\\.";
    public static final String CHANGE_SPECIAL_FIELD = "change";
    public static final String FIELD_PAYLOAD_FIELD_CONF = "partition.payload.field";
    public static final String FIELD_TOPIC_PARTITION_NUM_CONF = "partition.topic.num";

    static final Field PARTITION_PAYLOAD_FIELDS_FIELD = Field.create(FIELD_PAYLOAD_FIELD_CONF)
            .withDisplayName("List of payload fields to use for compute partition.")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(
                    CommonConnectorConfig::notContainEmptyElements,
                    CommonConnectorConfig::notContainSpaceInAnyElements)
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
    private SmtManager<R> smtManager;
    private List<String> payloadFields;
    private int partitionNumber;

    @Override
    public ConfigDef config() {

        ConfigDef config = new ConfigDef();
        // group does not manage validator definition. Validation will not work here.
        return Field.group(config, "partitions",
                PARTITION_PAYLOAD_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD);
    }

    @Override
    public void configure(Map<String, ?> props) {

        final Configuration config = Configuration.from(props);

        smtManager = new SmtManager<>(config);

        smtManager.validate(config, Field.setOf(PARTITION_PAYLOAD_FIELDS_FIELD, TOPIC_PARTITION_NUM_FIELD));

        payloadFields = config.getList(PARTITION_PAYLOAD_FIELDS_FIELD);
        partitionNumber = config.getInteger(TOPIC_PARTITION_NUM_FIELD);
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

            if (isToSkip((SourceRecord) originalRecord)) {
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
            LOGGER.error("Error occurred while processing message {}. Skipping SMT", envelope);
            throw new ConnectException(String.format("Unprocessable message %s", envelope), e);
        }
    }

    private static boolean isToSkip(SourceRecord originalRecord) {
        return TRUNCATE.equals(Envelope.operationFor(originalRecord)) ||
                MESSAGE.equals(Envelope.operationFor(originalRecord));
    }

    private Optional<Object> toValue(String fieldName, Struct envelope) {

        try {
            String[] subFields = fieldName.split(NESTING_SEPARATOR);

            if (subFields.length == 1) {
                return Optional.of(envelope.get(subFields[0]));
            }

            Struct lastStruct = getLastStruct(envelope, subFields);

            return Optional.of(lastStruct.get(subFields[subFields.length - 1]));
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

    private int computePartition(Integer partitionNumber, List<Object> values) {
        // hashCode can be negative due to overflow. Since Math.abs(Integer.MIN_INT) will still return a negative number
        // we use bitwise operation to remove the sign

        Integer totalHashCode = values.stream().map(Object::hashCode).reduce(0, Integer::sum);
        return (totalHashCode & Integer.MAX_VALUE) % partitionNumber;
    }

    @Override
    public void close() {
    }
}
