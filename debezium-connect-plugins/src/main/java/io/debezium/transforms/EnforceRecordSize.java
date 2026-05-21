/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Field;
import io.debezium.metadata.ConfigDescriptor;
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
 * </ul>
 *
 * String size is estimated using str.length() as a constant-time approximation.
 * This assumes 1 byte per character, which understates multi-byte UTF-8 content
 * but avoids the O(n) cost of getBytes().
 *
 * @author Thomas Thornton
 */
public class EnforceRecordSize<R extends ConnectRecord<R>> implements Transformation<R>, Versioned, ConfigDescriptor {

    public static final String MAX_BYTES_CONF = "max.bytes";
    public static final String COMPRESSION_RATIO_CONF = "compression.ratio";
    public static final String MIN_FIELD_SIZE_CONF = "min.field.size";

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
                    MIN_FIELD_SIZE_FIELD.description());

    private int maxBytes;
    private double compressionRatio;
    private int minFieldSize;

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
                record.timestamp());
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
    }

    @Override
    public void configure(Map<String, ?> props) {
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
    }

    @Override
    public Field.Set getConfigFields() {
        return Field.setOf(MAX_BYTES_FIELD, COMPRESSION_RATIO_FIELD, MIN_FIELD_SIZE_FIELD);
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
}
