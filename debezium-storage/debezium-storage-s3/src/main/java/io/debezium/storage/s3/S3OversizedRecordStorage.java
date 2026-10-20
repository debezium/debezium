/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.spi.storage.OversizedRecord;
import io.debezium.spi.storage.OversizedRecordReference;
import io.debezium.spi.storage.OversizedRecordStorage;
import io.debezium.util.Strings;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Stores complete oversized records in Amazon S3.
 *
 * @author Debezium Authors
 */
@Incubating
public class S3OversizedRecordStorage implements OversizedRecordStorage {

    public static final String BASE_PATH_CONFIG = "base.path";
    public static final String REGION_CONFIG = "region";
    public static final String ENDPOINT_CONFIG = "endpoint";
    public static final String FORCE_PATH_STYLE_CONFIG = "force.path.style";
    public static final String ACCESS_KEY_ID_CONFIG = "access.key.id";
    public static final String SECRET_ACCESS_KEY_CONFIG = "secret.access.key";

    public static final Field BASE_PATH = Field.create(BASE_PATH_CONFIG)
            .withDisplayName("S3 base path")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The S3 bucket and optional object prefix, in the form s3://bucket/optional-prefix.");

    public static final Field REGION = Field.create(REGION_CONFIG)
            .withDisplayName("S3 region")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .withDescription("The AWS region in which the bucket is located.");

    public static final Field ENDPOINT = Field.create(ENDPOINT_CONFIG)
            .withDisplayName("S3 endpoint")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("An optional absolute URI of a custom S3-compatible endpoint.");

    public static final Field FORCE_PATH_STYLE = Field.create(FORCE_PATH_STYLE_CONFIG)
            .withDisplayName("S3 force path style URL")
            .withType(Type.BOOLEAN)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withValidation(Field::isBoolean)
            .withDescription("Whether the S3 client uses path-style access.");

    public static final Field ACCESS_KEY_ID = Field.create(ACCESS_KEY_ID_CONFIG)
            .withDisplayName("S3 access key id")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("An optional static access key id. When omitted, the AWS default credentials provider chain is used.");

    public static final Field SECRET_ACCESS_KEY = Field.create(SECRET_ACCESS_KEY_CONFIG)
            .withDisplayName("S3 secret access key")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("An optional static secret access key. Supply it through a Kafka Connect ConfigProvider rather than as a literal value.");

    public static final Field.Set ALL_FIELDS = Field.setOf(BASE_PATH, REGION, ENDPOINT, FORCE_PATH_STYLE, ACCESS_KEY_ID, SECRET_ACCESS_KEY);

    private static final Logger LOGGER = LoggerFactory.getLogger(S3OversizedRecordStorage.class);
    private static final String STORAGE_NAME = "s3";

    private String bucket;
    private String prefix;
    private S3Client s3Client;

    @Override
    public void configure(Configuration config) {
        close();

        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        URI basePath = parseBasePath(config.getString(BASE_PATH));
        String regionName = config.getString(REGION);
        if (Strings.isNullOrBlank(regionName)) {
            throw new DebeziumException("Configuration '" + REGION_CONFIG + "' is required for " + getClass().getSimpleName());
        }

        String configuredBucket = basePath.getHost();
        String configuredPrefix = normalizePrefix(basePath.getPath());
        S3Client configuredClient = createS3Client(
                config,
                Region.of(regionName),
                createCredentialsProvider(config));

        bucket = configuredBucket;
        prefix = configuredPrefix;
        s3Client = configuredClient;
    }

    protected S3Client createS3Client(Configuration config, Region region, AwsCredentialsProvider credentialsProvider) {
        S3ClientBuilder builder = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .forcePathStyle(config.getBoolean(FORCE_PATH_STYLE));

        String endpoint = config.getString(ENDPOINT);
        if (!Strings.isNullOrBlank(endpoint)) {
            builder.endpointOverride(parseAbsoluteUri(ENDPOINT_CONFIG, endpoint));
        }
        return builder.build();
    }

    private static AwsCredentialsProvider createCredentialsProvider(Configuration config) {
        String accessKeyId = config.getString(ACCESS_KEY_ID);
        String secretAccessKey = config.getString(SECRET_ACCESS_KEY);
        if (!Strings.isNullOrBlank(accessKeyId) && !Strings.isNullOrBlank(secretAccessKey)) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
        return DefaultCredentialsProvider.create();
    }

    @Override
    public OversizedRecordReference write(OversizedRecord record) {
        if (s3Client == null) {
            throw new DebeziumException(getClass().getSimpleName() + " is not configured");
        }

        byte[] payload = record.payload();
        String objectKey = prefix + record.key();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(objectKey)
                .contentType(record.contentType())
                .contentLength((long) payload.length)
                .build();

        try {
            s3Client.putObject(request, RequestBody.fromBytes(payload));
        }
        catch (RuntimeException e) {
            throw new DebeziumException("Failed to store oversized record at s3://" + bucket + "/" + objectKey, e);
        }

        return new OversizedRecordReference(STORAGE_NAME, objectUri(objectKey), payload.length);
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    private static URI parseBasePath(String value) {
        if (Strings.isNullOrBlank(value)) {
            throw new DebeziumException("Configuration '" + BASE_PATH_CONFIG + "' is required for "
                    + S3OversizedRecordStorage.class.getSimpleName());
        }

        URI uri = parseAbsoluteUri(BASE_PATH_CONFIG, value);
        if (!STORAGE_NAME.equalsIgnoreCase(uri.getScheme()) || Strings.isNullOrBlank(uri.getHost())
                || uri.getUserInfo() != null || uri.getPort() != -1 || uri.getQuery() != null || uri.getFragment() != null) {
            throw new DebeziumException("Configuration '" + BASE_PATH_CONFIG
                    + "' must be an S3 URI in the form s3://bucket/optional-prefix");
        }
        return uri;
    }

    private static URI parseAbsoluteUri(String property, String value) {
        try {
            URI uri = URI.create(value);
            if (!uri.isAbsolute()) {
                throw new IllegalArgumentException("URI is not absolute");
            }
            return uri;
        }
        catch (IllegalArgumentException e) {
            throw new DebeziumException("Configuration '" + property + "' must be a valid absolute URI", e);
        }
    }

    private static String normalizePrefix(String path) {
        if (Strings.isNullOrBlank(path) || "/".equals(path)) {
            return "";
        }
        String normalized = path;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized.endsWith("/") ? normalized : normalized + "/";
    }

    private URI objectUri(String objectKey) {
        try {
            return new URI(STORAGE_NAME, bucket, "/" + objectKey, null);
        }
        catch (URISyntaxException e) {
            throw new DebeziumException("Failed to create S3 reference for object key " + objectKey, e);
        }
    }
}
