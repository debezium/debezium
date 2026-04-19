/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3.processor;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
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

    public static final int DEFAULT_THRESHOLD_BYTES = 100 * 1024;

    private static final String INLINE_REFERENCE_PREFIX = "__debezium:s3:ref:v1:";
    private static final String INLINE_REFERENCE_DELIMITER = "|";

    private String bucket;
    private int thresholdBytes;
    private volatile S3Client s3Client;

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

        LOGGER.info("S3LargeMessagePostProcessor configured: bucket='{}', region='{}', thresholdBytes={}",
                bucket, regionName, thresholdBytes);

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
        final Schema originalSchema = recordStruct.schema();
        final List<org.apache.kafka.connect.data.Field> fields = originalSchema.fields();
        final List<String> fieldsToReplace = new ArrayList<>();
        for (org.apache.kafka.connect.data.Field field : fields) {
            final Object fieldValue = recordStruct.get(field);
            if (meetsThreshold(field.schema(), fieldValue)) {
                fieldsToReplace.add(field.name());
            }
        }

        if (fieldsToReplace.isEmpty()) {
            return;
        }

        LOGGER.debug("Offloading {} field(s) to S3 for {} struct: {}", fieldsToReplace.size(), qualifier, fieldsToReplace);

        for (org.apache.kafka.connect.data.Field field : fields) {
            final Object fieldValue = recordStruct.get(field);
            if (fieldsToReplace.contains(field.name())) {
                final String objectKey = buildObjectKey(source, field.name(), qualifier);
                final byte[] bytes = toBytes(field.schema(), fieldValue);

                uploadToS3(objectKey, bytes, field.schema().type());
                recordStruct.put(field.name(), toInlineReferenceValue(field.schema(), objectKey));

                LOGGER.debug("Field '{}' ({} bytes) offloaded to S3 key '{}'.", field.name(), bytes.length, objectKey);
            }
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
            case STRING -> ((String) value).getBytes(StandardCharsets.UTF_8).length >= thresholdBytes;
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

    String buildObjectKey(Struct source, String fieldName, String qualifier) {
        final StringBuilder sb = new StringBuilder();

        if (source != null) {
            appendSourceField(sb, source, "db");
            appendSourceField(sb, source, "table");
            appendSourceField(sb, source, "schema");
            appendSourceField(sb, source, "file");
            appendSourceField(sb, source, "pos");
            appendSourceField(sb, source, "row");
            appendSourceField(sb, source, "lsn");
            appendSourceField(sb, source, "scn");
            appendSourceField(sb, source, "commit_scn");
            appendSourceField(sb, source, "ts_ms");
            appendSourceField(sb, source, "gtid");
            appendSourceField(sb, source, "event");
        }
        else {
            sb.append("unknown-source");
        }

        sb.append('/').append(fieldName)
                .append('/').append(qualifier);

        return sb.toString();
    }

    private void appendSourceField(StringBuilder sb, Struct source, String fieldName) {
        final org.apache.kafka.connect.data.Field field = source.schema().field(fieldName);
        if (field != null) {
            final Object val = source.get(field);
            if (val != null) {
                sb.append('/').append(fieldName).append('=').append(val);
            }
        }
    }

    private void uploadToS3(String objectKey, byte[] bytes, Schema.Type originalType) {
        final PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .contentType(originalType == Schema.Type.STRING ? "text/plain; charset=utf-8" : "application/octet-stream")
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(bytes));
    }

    private static Struct getFieldStruct(Struct parent, String fieldName) {
        final org.apache.kafka.connect.data.Field field = parent.schema().field(fieldName);
        if (field == null) {
            return null;
        }
        return parent.getStruct(fieldName);
    }
}
