/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3.processor;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.data.Envelope;
import io.debezium.function.Predicates;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.util.Strings;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Incubating
public class S3LargeMessagePostProcessor implements PostProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3LargeMessagePostProcessor.class);

    public static final String BUCKET_NAME_CONFIG = "large.message.s3.bucket.name";
    public static final String REGION_NAME_CONFIG = "large.message.s3.region.name";
    public static final String THRESHOLD_BYTES_CONFIG = "large.message.s3.threshold.bytes";
    public static final String ACCESS_KEY_ID_CONFIG = "large.message.s3.access.key.id";
    public static final String SECRET_ACCESS_KEY_CONFIG = "large.message.s3.secret.access.key";
    public static final String ENDPOINT_CONFIG = "large.message.s3.endpoint";
    public static final String COLUMNS_INCLUDE_LIST_CONFIG = "large.message.s3.columns.include.list";
    public static final String COLUMNS_EXCLUDE_LIST_CONFIG = "large.message.s3.columns.exclude.list";
    public static final String ERROR_HANDLING_MODE_CONFIG = "large.message.s3.error.handling.mode";

    public static final int DEFAULT_THRESHOLD_BYTES = 100 * 1024;

    private static final String INLINE_REFERENCE_PREFIX = "__debezium:s3:ref:v1:";
    private static final String INLINE_REFERENCE_DELIMITER = "|";

    private String bucket;
    private int thresholdBytes;
    private S3Client s3Client;
    private Predicate<String> selector;
    private ErrorHandlingMode errorHandlingMode;

    /**
     * Specifies how the post processor reacts to S3 upload failures.
     */
    public enum ErrorHandlingMode implements EnumeratedValue {
        /**
         * Log a warning when an S3 upload fails and pass the field value through as-is (not offloaded).
         */
        WARN("warn"),
        /**
         * Fail the connector by throwing a {@link DebeziumException} when an S3 upload fails.
         */
        FAIL("fail");

        private final String value;

        ErrorHandlingMode(String value) {
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
        public static ErrorHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (ErrorHandlingMode option : ErrorHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    @Override
    public void configure(Map<String, ?> properties) {
        final Configuration config = Configuration.from(properties);

        bucket = config.getString(BUCKET_NAME_CONFIG);
        if (Strings.isNullOrBlank(bucket)) {
            throw new DebeziumException("Configuration '" + BUCKET_NAME_CONFIG + "' is required for " + getClass().getSimpleName());
        }

        final String regionName = config.getString(REGION_NAME_CONFIG);
        if (Strings.isNullOrBlank(regionName)) {
            throw new DebeziumException("Configuration '" + REGION_NAME_CONFIG + "' is required for " + getClass().getSimpleName());
        }

        thresholdBytes = config.getInteger(THRESHOLD_BYTES_CONFIG, DEFAULT_THRESHOLD_BYTES);
        if (thresholdBytes <= 0) {
            throw new DebeziumException("Configuration '" + THRESHOLD_BYTES_CONFIG + "' must be a positive integer.");
        }

        errorHandlingMode = ErrorHandlingMode.parse(config.getString(ERROR_HANDLING_MODE_CONFIG));
        if (errorHandlingMode == null) {
            errorHandlingMode = ErrorHandlingMode.WARN;
        }

        selector = new ColumnPredicateBuilder()
                .includeColumns(config.getString(COLUMNS_INCLUDE_LIST_CONFIG))
                .excludeColumns(config.getString(COLUMNS_EXCLUDE_LIST_CONFIG))
                .build();

        LOGGER.info("S3LargeMessagePostProcessor configured: bucket='{}', region='{}', thresholdBytes={}, errorHandlingMode={}",
                bucket, regionName, thresholdBytes, errorHandlingMode.getValue());

        final Region region = Region.of(regionName);
        final AwsCredentialsProvider credentialsProvider = createCredentialsProvider(config);

        s3Client = createS3Client(config, region, credentialsProvider);
    }

    protected S3Client createS3Client(Configuration config, Region region, AwsCredentialsProvider credentialsProvider) {
        S3ClientBuilder builder = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider);

        final String endpointStr = config.getString(ENDPOINT_CONFIG);
        if (!Strings.isNullOrBlank(endpointStr)) {
            LOGGER.info("Using custom S3 endpoint: {}", endpointStr);
            builder.endpointOverride(URI.create(endpointStr));
            builder.forcePathStyle(true);
        }

        return builder.build();
    }

    private AwsCredentialsProvider createCredentialsProvider(Configuration config) {
        final String accessKeyId = config.getString(ACCESS_KEY_ID_CONFIG);
        final String secretAccessKey = config.getString(SECRET_ACCESS_KEY_CONFIG);
        if (!Strings.isNullOrBlank(accessKeyId) && !Strings.isNullOrBlank(secretAccessKey)) {
            LOGGER.info("Using StaticCredentialsProvider for S3 authentication.");
            AwsCredentials credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
            return StaticCredentialsProvider.create(credentials);
        }
        LOGGER.info("Using DefaultCredentialsProvider for S3 authentication.");
        return DefaultCredentialsProvider.create();
    }

    @Override
    public void apply(Object key, Struct value) {
        if (value == null) {
            LOGGER.debug("Skipping null change event value.");
            return;
        }

        final Struct source = getFieldStruct(value, Envelope.FieldName.SOURCE);

        final Struct before = getFieldStruct(value, Envelope.FieldName.BEFORE);
        final Struct after = getFieldStruct(value, Envelope.FieldName.AFTER);

        if (before != null) {
            processRecordStruct(before, source, "before");
        }
        if (after != null) {
            processRecordStruct(after, source, "after");
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            LOGGER.debug("Closing S3 client.");
            s3Client.close();
        }
    }

    private void processRecordStruct(Struct recordStruct, Struct source, String qualifier) {
        int offloadedCount = 0;

        for (org.apache.kafka.connect.data.Field field : recordStruct.schema().fields()) {
            final Object fieldValue = recordStruct.get(field);
            if (!meetsThreshold(field.schema(), fieldValue)) {
                continue;
            }
            if (!selector.test(field.name())) {
                continue;
            }

            final String objectKey = buildObjectKey(source, field.name(), qualifier);
            if (objectKey == null) {
                LOGGER.warn("Unable to resolve a unique S3 object key for field '{}' in '{}' struct; " +
                        "the source info block is missing or empty. The field value will be passed through as-is " +
                        "(not offloaded to S3) to avoid potential data loss from object-key collisions. " +
                        "Please ensure the connector provides a source info block with fields that uniquely " +
                        "identify each event.",
                        field.name(), qualifier);
                continue;
            }
            final byte[] bytes = toBytes(field.schema(), fieldValue);

            if (!uploadToS3(objectKey, bytes, field.schema().type(), field.name(), qualifier)) {
                continue;
            }
            recordStruct.put(field.name(), toInlineReferenceValue(field.schema(), objectKey));

            LOGGER.debug("Field '{}' ({} bytes) offloaded to S3 key '{}'.", field.name(), bytes.length, objectKey);
            offloadedCount++;
        }

        if (offloadedCount > 0) {
            LOGGER.debug("Offloaded {} field(s) to S3 for {} struct", offloadedCount, qualifier);
        }
    }

    private Object toInlineReferenceValue(Schema schema, String objectKey) {
        final String reference = encodeInlineReference(objectKey);
        return switch (schema.type()) {
            case STRING -> reference;
            case BYTES -> reference.getBytes(StandardCharsets.UTF_8);
            default -> throw new DebeziumException("Unsupported field type for inline S3 reference: " + schema.type());
        };
    }

    private String encodeInlineReference(String objectKey) {
        return INLINE_REFERENCE_PREFIX + bucket + INLINE_REFERENCE_DELIMITER + objectKey;
    }

    private boolean meetsThreshold(Schema schema, Object value) {
        if (value == null) {
            return false;
        }
        return switch (schema.type()) {
            case STRING -> {
                final String s = (String) value;
                yield s.length() >= thresholdBytes || s.getBytes(StandardCharsets.UTF_8).length >= thresholdBytes;
            }
            case BYTES -> meetsThresholdForBytes(value);
            default -> false;
        };
    }

    private boolean meetsThresholdForBytes(Object value) {
        if (value instanceof byte[] byteData) {
            return byteData.length >= thresholdBytes;
        }
        if (value instanceof ByteBuffer buffer) {
            return buffer.remaining() >= thresholdBytes;
        }
        return false;
    }

    private byte[] toBytes(Schema schema, Object value) {
        return switch (schema.type()) {
            case STRING -> ((String) value).getBytes(StandardCharsets.UTF_8);
            case BYTES -> toBytesForBytes(value);
            default -> throw new DebeziumException("Unsupported field type for S3 offloading: " + schema.type());
        };
    }

    private byte[] toBytesForBytes(Object value) {
        if (value instanceof byte[] byteData) {
            return byteData;
        }
        if (value instanceof ByteBuffer buffer) {
            final ByteBuffer buf = buffer.duplicate();
            final byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }
        throw new DebeziumException("Unsupported field type for S3 offloading: BYTES");
    }

    /**
     * Builds a unique S3 object key for a large field value by iterating over the
     * fields present in the source information block schema.
     * <p>
     * The source information block is connector-specific, so rather than hardcoding
     * field names, this method dynamically iterates all fields defined in the source
     * schema and incorporates their names and values into the object key.
     * <p>
     * <b>Uniqueness requirement:</b> Connector implementations must ensure that the
     * source information block contains a set of fields whose values uniquely identify
     * each change event. If two events from the same source produce identical source
     * block values, their object keys will collide, causing one event's offloaded value
     * to overwrite the other — leading to data loss. The core Debezium connectors
     * satisfy this requirement, but non-core connectors may not; users of this post
     * processor should verify their connector's source block is unique per event.
     * <p>
     * If the source information block is {@code null} or contains no fields, a unique
     * object key cannot be resolved and this method returns {@code null}. In that case
     * the caller should leave the original field value in place (pass it through as-is)
     * rather than risk a collision-based data loss scenario.
     *
     * @param source the source information block struct, may be {@code null}
     * @param fieldName the name of the field being offloaded
     * @param qualifier the struct qualifier (e.g., {@code "before"} or {@code "after"})
     * @return a unique S3 object key, or {@code null} if a unique key cannot be resolved
     */
    String buildObjectKey(Struct source, String fieldName, String qualifier) {
        final StringBuilder sb = new StringBuilder();

        if (source != null) {
            for (org.apache.kafka.connect.data.Field field : source.schema().fields()) {
                final Object val = source.get(field);
                if (val != null) {
                    if (!sb.isEmpty()) {
                        sb.append('/');
                    }
                    sb.append(field.name()).append('=').append(encodeValue(val));
                }
            }
        }

        if (sb.isEmpty()) {
            return null;
        }

        sb.append('/').append(encodeValue(fieldName))
                .append('/').append(qualifier);

        return sb.toString();
    }

    private static String encodeValue(Object value) {
        return URLEncoder.encode(String.valueOf(value), StandardCharsets.UTF_8);
    }

    private boolean uploadToS3(String objectKey, byte[] bytes, Schema.Type originalType, String fieldName, String qualifier) {
        try {
            final PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .contentType(originalType == Schema.Type.STRING ? "text/plain; charset=utf-8" : "application/octet-stream")
                    .build();
            s3Client.putObject(request, RequestBody.fromBytes(bytes));
            return true;
        }
        catch (Exception e) {
            switch (errorHandlingMode) {
                case FAIL:
                    throw new DebeziumException("Failed to upload field '" + fieldName + "' in '" + qualifier
                            + "' struct to S3 key '" + objectKey + "'", e);
                case WARN:
                    LOGGER.warn("Failed to upload field '{}' in '{}' struct to S3 key '{}'; " +
                            "the field value will be passed through as-is (not offloaded to S3).",
                            fieldName, qualifier, objectKey, e);
                    return false;
                default:
                    return false;
            }
        }
    }

    private static Struct getFieldStruct(Struct parent, String fieldName) {
        final org.apache.kafka.connect.data.Field field = parent.schema().field(fieldName);
        if (field == null) {
            return null;
        }
        return parent.getStruct(fieldName);
    }

    /**
     * Builds a predicate that filters field names based on optional include/exclude lists.
     * <p>
     * If an include list is provided, only fields whose names match the list are eligible
     * for offloading. If an exclude list is provided, fields whose names match are
     * excluded from offloading. If neither is provided, all fields are eligible.
     */
    private static class ColumnPredicateBuilder {

        private Predicate<String> columnInclusions;
        private Predicate<String> columnExclusions;

        public ColumnPredicateBuilder includeColumns(String columnNames) {
            if (Strings.isNullOrBlank(columnNames)) {
                columnInclusions = null;
            }
            else {
                columnInclusions = Predicates.includes(columnNames, Pattern.CASE_INSENSITIVE);
            }
            return this;
        }

        public ColumnPredicateBuilder excludeColumns(String columnNames) {
            if (Strings.isNullOrBlank(columnNames)) {
                columnExclusions = null;
            }
            else {
                columnExclusions = Predicates.excludes(columnNames, Pattern.CASE_INSENSITIVE);
            }
            return this;
        }

        public Predicate<String> build() {
            if (columnInclusions != null) {
                return columnInclusions;
            }
            if (columnExclusions != null) {
                return columnExclusions;
            }
            return (x) -> true;
        }
    }
}
