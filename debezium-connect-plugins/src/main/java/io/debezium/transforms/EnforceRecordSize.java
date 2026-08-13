/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Instantiator;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.spi.storage.OversizedRecord;
import io.debezium.spi.storage.OversizedRecordReference;
import io.debezium.spi.storage.OversizedRecordStorage;
import io.debezium.util.ApproximateStructSizeCalculator;

/**
 * A Single Message Transform that enforces a maximum record size.
 *
 * This is useful when downstream systems have a maximum message size limit.
 * The transform estimates the serialized size of the record and, if it exceeds
 * the configured maximum, applies a size reduction strategy.
 *
 * Supported strategies:
 * <ul>
 *   <li>Proportional column truncation: truncates string/bytes columns proportionally
 *       (larger columns are truncated more). Columns at or below the configured
 *       minimum field size are excluded from truncation.</li>
 *   <li>Claim check: writes the complete source record through a configured storage
 *       implementation and replaces selected columns with a durable reference.</li>
 * </ul>
 *
 * String size is estimated using str.length() as a constant-time approximation.
 * This assumes 1 byte per character, which understates multi-byte UTF-8 content
 * but avoids the O(n) cost of getBytes().
 *
 * @author Thomas Thornton
 */
public class EnforceRecordSize<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String MAX_BYTES_CONF = "max.bytes";
    public static final String COMPRESSION_RATIO_CONF = "compression.ratio";
    public static final String MIN_FIELD_SIZE_CONF = "min.field.size";
    public static final String STRATEGY_CONF = "strategy";
    public static final String CLAIM_CHECK_STORAGE_CLASS_CONF = "claim.check.storage.class";
    public static final String CLAIM_CHECK_COLUMNS_CONF = "claim.check.columns.include.list";
    public static final String CLAIM_CHECK_STORAGE_CONFIG_PREFIX = "claim.check.storage.";

    public enum Strategy implements EnumeratedValue {
        TRUNCATE("truncate"),
        CLAIM_CHECK("claim_check");

        private final String value;

        Strategy(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        static Strategy parse(String value) {
            for (Strategy strategy : values()) {
                if (strategy.value.equalsIgnoreCase(value)) {
                    return strategy;
                }
            }
            return null;
        }
    }

    private static final Field MAX_BYTES_FIELD = Field.create(MAX_BYTES_CONF)
            .withDisplayName("Maximum record size")
            .withType(ConfigDef.Type.INT)
            .withImportance(ConfigDef.Importance.HIGH)
            .required()
            .withDescription("The maximum record size in bytes. Records exceeding this size will have their " +
                    "string and bytes columns truncated proportionally to fit within this limit.");

    private static final Field COMPRESSION_RATIO_FIELD = Field.create(COMPRESSION_RATIO_CONF)
            .withDisplayName("Compression ratio")
            .withType(ConfigDef.Type.DOUBLE)
            .withDefault("1.0")
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Ratio to account for record serialization differences. The estimated record size " +
                    "is multiplied by this ratio before comparing to the max size. " +
                    "For example, if your serialization compresses raw record size by 50%, " +
                    "set this to 0.5. Downstream systems typically provide metrics to discover " +
                    "the effective ratio, e.g. Kafka exposes " +
                    "kafka.producer:type=producer-metrics,client-id=<id>/compression-rate-avg. " +
                    "Default is 1.0 (no adjustment).");

    private static final Field MIN_FIELD_SIZE_FIELD = Field.create(MIN_FIELD_SIZE_CONF)
            .withDisplayName("Minimum field size")
            .withType(ConfigDef.Type.INT)
            .withDefault(25000)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Fields with a size at or below this value (in bytes) will not be truncated. " +
                    "Only fields larger than this threshold are eligible for proportional truncation. " +
                    "Default is 25000 (25KB).");

    private static final Field STRATEGY_FIELD = Field.create(STRATEGY_CONF)
            .withDisplayName("Record size enforcement strategy")
            .withEnum(Strategy.class, Strategy.TRUNCATE)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Strategy used for records that exceed 'max.bytes'. The default 'truncate' strategy " +
                    "retains the existing proportional truncation behavior. The 'claim_check' strategy stores the " +
                    "complete record externally and emits durable references in configured columns.");

    private static final Field CLAIM_CHECK_STORAGE_CLASS_FIELD = Field.create(CLAIM_CHECK_STORAGE_CLASS_CONF)
            .withDisplayName("Claim-check storage class")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Fully-qualified OversizedRecordStorage implementation class. Required when strategy is 'claim_check'.");

    private static final Field CLAIM_CHECK_COLUMNS_FIELD = Field.create(CLAIM_CHECK_COLUMNS_CONF)
            .withDisplayName("Claim-check columns include list")
            .withType(ConfigDef.Type.LIST)
            .withDefault("")
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Comma-separated columns to replace with claim-check markers. Qualified column names are accepted; " +
                    "matching uses the final column-name segment. Required when strategy is 'claim_check'.");

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MAX_BYTES_CONF,
                    ConfigDef.Type.INT,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    MAX_BYTES_FIELD.description())
            .define(COMPRESSION_RATIO_CONF,
                    ConfigDef.Type.DOUBLE,
                    1.0,
                    ConfigDef.Importance.MEDIUM,
                    COMPRESSION_RATIO_FIELD.description())
            .define(MIN_FIELD_SIZE_CONF,
                    ConfigDef.Type.INT,
                    25000,
                    ConfigDef.Importance.MEDIUM,
                    MIN_FIELD_SIZE_FIELD.description())
            .define(STRATEGY_CONF,
                    ConfigDef.Type.STRING,
                    Strategy.TRUNCATE.getValue(),
                    ConfigDef.ValidString.in(Strategy.TRUNCATE.getValue(), Strategy.CLAIM_CHECK.getValue()),
                    ConfigDef.Importance.HIGH,
                    STRATEGY_FIELD.description())
            .define(CLAIM_CHECK_STORAGE_CLASS_CONF,
                    ConfigDef.Type.STRING,
                    null,
                    ConfigDef.Importance.HIGH,
                    CLAIM_CHECK_STORAGE_CLASS_FIELD.description())
            .define(CLAIM_CHECK_COLUMNS_CONF,
                    ConfigDef.Type.LIST,
                    "",
                    ConfigDef.Importance.HIGH,
                    CLAIM_CHECK_COLUMNS_FIELD.description());

    private int maxBytes;
    private double compressionRatio;
    private int minFieldSize;
    private Strategy strategy = Strategy.TRUNCATE;
    private Set<String> claimCheckColumns = Set.of();
    private OversizedRecordStorage claimCheckStorage;

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        if (!(record.value() instanceof Struct)) {
            return record;
        }

        if (!(record instanceof SourceRecord)) {
            return record;
        }

        long rawEstimate = ApproximateStructSizeCalculator.getApproximateRecordSize((SourceRecord) record);
        long currentSize = (long) Math.ceil(rawEstimate * compressionRatio);
        if (currentSize <= maxBytes) {
            return record;
        }

        if (strategy == Strategy.CLAIM_CHECK) {
            return applyClaimCheck(record);
        }

        Struct value = (Struct) record.value();
        long excess = currentSize - maxBytes;

        List<TruncatableField> beforeFields = getTruncatableFields(value, "before");
        List<TruncatableField> afterFields = getTruncatableFields(value, "after");

        long beforeBytes = beforeFields.stream().mapToLong(f -> f.sizeBytes).sum();
        long afterBytes = afterFields.stream().mapToLong(f -> f.sizeBytes).sum();
        long totalBytes = beforeBytes + afterBytes;

        if (totalBytes <= 0) {
            return record;
        }

        long beforeExcess = (long) Math.ceil((double) beforeBytes / totalBytes * excess);
        long afterExcess = (long) Math.ceil((double) afterBytes / totalBytes * excess);

        truncateFields(value, "before", beforeFields, beforeBytes, beforeExcess);
        truncateFields(value, "after", afterFields, afterBytes, afterExcess);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                value,
                record.timestamp(),
                record.headers());
    }

    private R applyClaimCheck(R record) {
        SourceRecord sourceRecord = (SourceRecord) record;
        Struct value = (Struct) sourceRecord.value();
        List<ClaimCheckField> fields = findClaimCheckFields(value);

        validateClaimCheckFields(value, fields, sourceRecord.topic());

        ClaimCheckRecordSerializer.SerializedRecord serialized = ClaimCheckRecordSerializer.serialize(sourceRecord);
        OversizedRecordReference reference;
        try {
            reference = claimCheckStorage.write(new OversizedRecord(
                    serialized.key(),
                    serialized.payload(),
                    "application/json"));
        }
        catch (RuntimeException e) {
            throw new ConnectException("Failed to store oversized record for topic " + sourceRecord.topic(), e);
        }
        if (reference == null) {
            throw new ConnectException("Claim-check storage returned no reference for topic " + sourceRecord.topic());
        }

        Struct replacementValue = copyStruct(value);
        for (ClaimCheckField field : fields) {
            Struct section = replacementValue.getStruct(field.sectionName);
            section.put(field.fieldName, claimCheckMarker(reference, serialized.sha256(), field));
        }

        R replacement = record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                replacementValue,
                record.timestamp(),
                record.headers());

        long replacementSize = (long) Math.ceil(
                ApproximateStructSizeCalculator.getApproximateRecordSize((SourceRecord) replacement) * compressionRatio);
        if (replacementSize > maxBytes) {
            throw new ConnectException("Claim-check replacement for topic " + sourceRecord.topic()
                    + " still exceeds max.bytes: " + replacementSize + " > " + maxBytes);
        }
        return replacement;
    }

    private List<ClaimCheckField> findClaimCheckFields(Struct envelope) {
        List<ClaimCheckField> fields = new ArrayList<>();
        collectClaimCheckFields(envelope, "before", fields);
        collectClaimCheckFields(envelope, "after", fields);
        return fields;
    }

    private void collectClaimCheckFields(Struct envelope, String sectionName, List<ClaimCheckField> fields) {
        org.apache.kafka.connect.data.Field sectionField = envelope.schema().field(sectionName);
        if (sectionField == null) {
            return;
        }
        Object sectionValue = envelope.getWithoutDefault(sectionName);
        if (!(sectionValue instanceof Struct section)) {
            return;
        }

        for (org.apache.kafka.connect.data.Field field : section.schema().fields()) {
            if (!claimCheckColumns.contains(field.name())) {
                continue;
            }
            Object fieldValue = section.getWithoutDefault(field.name());
            if (fieldValue != null) {
                fields.add(new ClaimCheckField(sectionName, field.name(), field.schema().type()));
            }
        }
    }

    private void validateClaimCheckFields(Struct envelope, List<ClaimCheckField> fields, String topic) {
        Set<String> availableColumns = new LinkedHashSet<>();
        collectSectionColumns(envelope, "before", availableColumns);
        collectSectionColumns(envelope, "after", availableColumns);

        Set<String> missingColumns = claimCheckColumns.stream()
                .filter(column -> !availableColumns.contains(column))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (!missingColumns.isEmpty()) {
            throw new ConnectException("Claim-check columns " + missingColumns + " were not found in record for topic " + topic);
        }
        if (fields.isEmpty()) {
            throw new ConnectException("Claim-check columns are present but null in record for topic " + topic);
        }

        validateClaimCheckColumnSchemas(envelope, "before");
        validateClaimCheckColumnSchemas(envelope, "after");
    }

    private void validateClaimCheckColumnSchemas(Struct envelope, String sectionName) {
        org.apache.kafka.connect.data.Field sectionField = envelope.schema().field(sectionName);
        if (sectionField == null || sectionField.schema().type() != Schema.Type.STRUCT) {
            return;
        }
        for (org.apache.kafka.connect.data.Field field : sectionField.schema().fields()) {
            if (claimCheckColumns.contains(field.name())
                    && field.schema().type() != Schema.Type.STRING
                    && field.schema().type() != Schema.Type.BYTES) {
                throw new ConnectException("Claim-check column '" + field.name() + "' has schema type "
                        + field.schema().type() + "; only STRING and BYTES columns are supported");
            }
        }
    }

    private static void collectSectionColumns(Struct envelope, String sectionName, Set<String> columns) {
        org.apache.kafka.connect.data.Field sectionField = envelope.schema().field(sectionName);
        if (sectionField == null || sectionField.schema().type() != Schema.Type.STRUCT) {
            return;
        }
        sectionField.schema().fields().forEach(field -> columns.add(field.name()));
    }

    private static Struct copyStruct(Struct value) {
        Struct copy = new Struct(value.schema());
        for (org.apache.kafka.connect.data.Field field : value.schema().fields()) {
            Object fieldValue = value.getWithoutDefault(field.name());
            copy.put(field.name(), fieldValue instanceof Struct struct ? copyStruct(struct) : fieldValue);
        }
        return copy;
    }

    private static Object claimCheckMarker(OversizedRecordReference reference, String sha256, ClaimCheckField field) {
        Map<String, Object> marker = new LinkedHashMap<>();
        marker.put("__debezium_claim_check", true);
        marker.put("version", 1);
        marker.put("storage", reference.storage());
        marker.put("uri", reference.uri().toString());
        marker.put("column", field.fieldName);
        marker.put("size_bytes", reference.sizeBytes());
        marker.put("sha256", sha256);

        try {
            String json = OBJECT_MAPPER.writeValueAsString(marker);
            return field.schemaType == Schema.Type.BYTES ? json.getBytes(StandardCharsets.UTF_8) : json;
        }
        catch (JsonProcessingException e) {
            throw new DebeziumException("Failed to serialize claim-check marker", e);
        }
    }

    private List<TruncatableField> getTruncatableFields(Struct envelope, String fieldName) {
        Schema envelopeSchema = envelope.schema();
        if (envelopeSchema.field(fieldName) == null) {
            return List.of();
        }
        Object fieldValue = envelope.get(fieldName);
        if (!(fieldValue instanceof Struct)) {
            return List.of();
        }
        return findTruncatableFields((Struct) fieldValue);
    }

    private void truncateFields(Struct envelope, String fieldName, List<TruncatableField> truncatableFields,
                                long totalTruncatableBytes, long excess) {
        if (truncatableFields.isEmpty() || totalTruncatableBytes == 0 || excess <= 0) {
            return;
        }

        Struct dataStruct = (Struct) envelope.get(fieldName);

        for (TruncatableField field : truncatableFields) {
            double proportion = (double) field.sizeBytes / totalTruncatableBytes;
            int bytesToRemove = (int) Math.ceil(proportion * excess);
            int newSizeBytes = Math.max(0, field.sizeBytes - bytesToRemove);

            truncateField(dataStruct, field, newSizeBytes);
        }
    }

    private static void truncateField(Struct dataStruct, TruncatableField field, int newSizeBytes) {
        if (field.value instanceof String) {
            String original = (String) field.value;
            if (original.length() > newSizeBytes) {
                dataStruct.put(field.fieldName, original.substring(0, newSizeBytes));
            }
        }
        else if (field.value instanceof ByteBuffer) {
            ByteBuffer original = (ByteBuffer) field.value;
            if (original.limit() > newSizeBytes) {
                dataStruct.put(field.fieldName, ByteBuffer.wrap(toArray(original, 0, newSizeBytes)));
            }
        }
    }

    private static byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        }
        else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    private static int estimateStringSize(String str) {
        return str.length();
    }

    private static int estimateBytesSize(ByteBuffer buffer) {
        return buffer.limit();
    }

    private List<TruncatableField> findTruncatableFields(Struct dataStruct) {
        List<TruncatableField> result = new ArrayList<>();
        Schema schema = dataStruct.schema();

        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            Object value = dataStruct.get(field);
            if (value == null) {
                continue;
            }

            Schema.Type type = field.schema().type();

            if (type == Schema.Type.STRING) {
                int size = estimateStringSize((String) value);
                if (size > minFieldSize) {
                    result.add(new TruncatableField(field.name(), value, size));
                }
            }
            else if (type == Schema.Type.BYTES) {
                int size = estimateBytesSize((ByteBuffer) value);
                if (size > minFieldSize) {
                    result.add(new TruncatableField(field.name(), value, size));
                }
            }
        }

        return result;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        if (claimCheckStorage != null) {
            claimCheckStorage.close();
            claimCheckStorage = null;
        }
    }

    @Override
    public void configure(Map<String, ?> props) {
        close();
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);

        int maxSize = config.getInt(MAX_BYTES_CONF);
        if (maxSize <= 0) {
            throw new ConfigException(MAX_BYTES_CONF, maxSize, "Must be a positive integer");
        }
        this.maxBytes = maxSize;

        double ratio = config.getDouble(COMPRESSION_RATIO_CONF);
        if (ratio <= 0) {
            throw new ConfigException(COMPRESSION_RATIO_CONF, ratio, "Must be a positive number");
        }
        this.compressionRatio = ratio;

        int minField = config.getInt(MIN_FIELD_SIZE_CONF);
        if (minField < 0) {
            throw new ConfigException(MIN_FIELD_SIZE_CONF, minField, "Must be non-negative");
        }
        this.minFieldSize = minField;

        this.strategy = Strategy.parse(config.getString(STRATEGY_CONF));
        if (strategy == Strategy.CLAIM_CHECK) {
            configureClaimCheck(config, props);
        }
        else {
            this.claimCheckColumns = Set.of();
        }
    }

    private void configureClaimCheck(AbstractConfig config, Map<String, ?> props) {
        String storageClass = config.getString(CLAIM_CHECK_STORAGE_CLASS_CONF);
        if (storageClass == null || storageClass.isBlank()) {
            throw new ConfigException(CLAIM_CHECK_STORAGE_CLASS_CONF, storageClass,
                    "Must be set when strategy is 'claim_check'");
        }

        List<String> configuredColumns = config.getList(CLAIM_CHECK_COLUMNS_CONF);
        this.claimCheckColumns = configuredColumns.stream()
                .map(String::trim)
                .filter(column -> !column.isEmpty())
                .map(EnforceRecordSize::unqualifiedColumnName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (claimCheckColumns.isEmpty()) {
            throw new ConfigException(CLAIM_CHECK_COLUMNS_CONF, configuredColumns,
                    "Must contain at least one column when strategy is 'claim_check'");
        }

        Object storage;
        try {
            storage = Instantiator.getInstance(storageClass);
        }
        catch (IllegalArgumentException e) {
            throw new ConfigException("Unable to instantiate claim-check storage class " + storageClass, e);
        }
        if (!(storage instanceof OversizedRecordStorage oversizedRecordStorage)) {
            throw new ConfigException(CLAIM_CHECK_STORAGE_CLASS_CONF, storageClass,
                    "Class must implement " + OversizedRecordStorage.class.getName());
        }

        Map<String, Object> storageProperties = new LinkedHashMap<>();
        props.forEach((name, value) -> {
            if (name.startsWith(CLAIM_CHECK_STORAGE_CONFIG_PREFIX)
                    && !name.equals(CLAIM_CHECK_STORAGE_CLASS_CONF)) {
                storageProperties.put(name.substring(CLAIM_CHECK_STORAGE_CONFIG_PREFIX.length()), value);
            }
        });
        try {
            oversizedRecordStorage.configure(Configuration.from(storageProperties));
            this.claimCheckStorage = oversizedRecordStorage;
        }
        catch (RuntimeException e) {
            oversizedRecordStorage.close();
            throw new ConfigException("Unable to configure claim-check storage class " + storageClass, e);
        }
    }

    private static String unqualifiedColumnName(String columnName) {
        int lastDot = columnName.lastIndexOf('.');
        return lastDot == -1 ? columnName : columnName.substring(lastDot + 1);
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(
                MAX_BYTES_FIELD,
                COMPRESSION_RATIO_FIELD,
                MIN_FIELD_SIZE_FIELD,
                STRATEGY_FIELD,
                CLAIM_CHECK_STORAGE_CLASS_FIELD,
                CLAIM_CHECK_COLUMNS_FIELD);
    }

    private static class TruncatableField {
        final String fieldName;
        final Object value;
        final int sizeBytes;

        TruncatableField(String fieldName, Object value, int sizeBytes) {
            this.fieldName = fieldName;
            this.value = value;
            this.sizeBytes = sizeBytes;
        }
    }

    private record ClaimCheckField(String sectionName, String fieldName, Schema.Type schemaType) {
    }
}
