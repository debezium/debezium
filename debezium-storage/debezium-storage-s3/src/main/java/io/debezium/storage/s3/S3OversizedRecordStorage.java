/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3;

import java.net.URI;
import java.net.URISyntaxException;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
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

    private static final String STORAGE_NAME = "s3";

    private String bucket;
    private String prefix;
    private S3Client s3Client;

    @Override
    public void configure(Configuration config) {
        close();

        URI basePath = parseBasePath(config.getString(BASE_PATH_CONFIG));
        String regionName = config.getString(REGION_CONFIG);
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
                .forcePathStyle(config.getBoolean(FORCE_PATH_STYLE_CONFIG, false));

        String endpoint = config.getString(ENDPOINT_CONFIG);
        if (!Strings.isNullOrBlank(endpoint)) {
            builder.endpointOverride(parseAbsoluteUri(ENDPOINT_CONFIG, endpoint));
        }
        return builder.build();
    }

    private static AwsCredentialsProvider createCredentialsProvider(Configuration config) {
        String accessKeyId = config.getString(ACCESS_KEY_ID_CONFIG);
        String secretAccessKey = config.getString(SECRET_ACCESS_KEY_CONFIG);
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
