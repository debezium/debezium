/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3.history;

import java.io.InputStream;
import java.net.URI;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.history.AbstractFileBasedSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** A {@link SchemaHistory} implementation that records schema changes as normal {@link SourceRecord}s on the specified topic,
 * and that recovers the history by establishing a Kafka Consumer re-processing all messages on that topic.
 *
 *  This implementation provides caching {@link HistoryRecord} on the main memory in the case of recovering records.
 *  Since S3 does not support Append operation on the object level. {@link S3SchemaHistory#start()} fetches history log
 *  from S3 and store the {@link HistoryRecord} on the main memory. Also {@link S3SchemaHistory#storeRecord(HistoryRecord)}
 *  creates new history object everytime invokes on S3
 *
 * @author hossein.torabi
 */
@NotThreadSafe
public class S3SchemaHistory extends AbstractFileBasedSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SchemaHistory.class);
    public static final String ACCESS_KEY_ID_CONFIG = "s3.access.key.id";
    public static final String SECRET_ACCESS_KEY_CONFIG = "s3.secret.access.key";
    public static final String REGION_CONFIG = CONFIGURATION_FIELD_PREFIX_STRING + "s3.region.name";
    public static final String BUCKET_CONFIG = CONFIGURATION_FIELD_PREFIX_STRING + "s3.bucket.name";
    public static final String OBJECT_NAME_CONFIG = CONFIGURATION_FIELD_PREFIX_STRING + "s3.object.name";
    public static final String ENDPOINT_CONFIG = CONFIGURATION_FIELD_PREFIX_STRING + "s3.endpoint";
    public static final String OBJECT_CONTENT_TYPE = "text/plain";

    public static final Field ACCESS_KEY_ID = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + ACCESS_KEY_ID_CONFIG)
            .withDisplayName("S3 access key id")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH);

    public static final Field SECRET_ACCESS_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + SECRET_ACCESS_KEY_CONFIG)
            .withDisplayName("S3 secret access key")
            .withType(Type.PASSWORD)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH);

    public static final Field REGION = Field.create(REGION_CONFIG)
            .withDisplayName("S3 region")
            .withWidth(Width.LONG)
            .withType(Type.STRING)
            .withImportance(Importance.MEDIUM)
            .required();

    public static final Field BUCKET = Field.create(BUCKET_CONFIG)
            .withDisplayName("S3 bucket")
            .withType(Type.STRING)
            .withImportance(Importance.HIGH)
            .required();

    public static final Field OBJECT_NAME = Field.create(OBJECT_NAME_CONFIG)
            .withDisplayName("S3 Object name")
            .withType(Type.STRING)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("The name of the object under which the history is stored.");

    public static final Field ENDPOINT = Field.create(ENDPOINT_CONFIG)
            .withDisplayName("S3 endpoint")
            .withType(Type.STRING)
            .withImportance(Importance.LOW);

    public static final Field.Set ALL_FIELDS = Field.setOf(ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION, BUCKET, OBJECT_NAME, ENDPOINT);

    private String bucket;
    private String objectName;
    private Region region;
    private URI endpoint = null;
    private AwsCredentialsProvider credentialsProvider = null;

    private volatile S3Client client = null;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException(
                    "Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        bucket = config.getString(BUCKET);
        objectName = config.getString(OBJECT_NAME);
        final var regionName = config.getString(REGION);
        // Unknown value is not detected by Region.of
        region = Region.of(regionName);

        LOGGER.info("Database history will be stored in bucket '{}' under key '{}' using region '{}'", bucket, objectName, region);

        final var uriString = config.getString(ENDPOINT);
        if (uriString != null) {
            LOGGER.info("Using explicitly configured endpoint " + uriString);
            endpoint = URI.create(uriString);
        }

        if (config.getString(ACCESS_KEY_ID) == null && config.getString(SECRET_ACCESS_KEY) == null) {
            LOGGER.info("DefaultCredentialsProvider is used for authentication");
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        else {
            LOGGER.info("StaticCredentialsProvider is used for authentication");
            AwsCredentials credentials = AwsBasicCredentials.create(config.getString(ACCESS_KEY_ID), config.getString(SECRET_ACCESS_KEY));
            credentialsProvider = StaticCredentialsProvider.create(credentials);
        }
    }

    @Override
    protected void doPreStart() {
        if (client == null) {
            S3ClientBuilder clientBuilder = S3Client.builder().credentialsProvider(credentialsProvider).region(region);
            if (endpoint != null) {
                clientBuilder.endpointOverride(endpoint);
            }

            client = clientBuilder.build();
        }
    }

    @Override
    protected void doStart() {
        InputStream objectInputStream = null;
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectName)
                    .responseContentType(OBJECT_CONTENT_TYPE)
                    .build();
            objectInputStream = client.getObject(request, ResponseTransformer.toInputStream());
        }
        catch (NoSuchKeyException e) {
            // do nothing
        }
        catch (S3Exception e) {
            throw new SchemaHistoryException("Can't retrieve history object from S3", e);
        }

        if (objectInputStream != null) {
            toHistoryRecord(objectInputStream);
        }
    }

    @Override
    public void doStop() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    protected void doPreStoreRecord(HistoryRecord record) {
        if (client == null) {
            throw new SchemaHistoryException("No S3 client is available. Ensure that 'start()' is called before storing database history records.");
        }
    }

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectName)
                    .contentType(OBJECT_CONTENT_TYPE)
                    .build();
            client.putObject(request, RequestBody.fromBytes(fromHistoryRecord(record)));
        }
        catch (S3Exception e) {
            throw new SchemaHistoryException("Can not store record to S3", e);
        }
    }

    @Override
    public boolean storageExists() {
        final boolean bucketExists = client.listBuckets().buckets().stream().map(Bucket::name).anyMatch(bucket::equals);
        if (bucketExists) {
            LOGGER.info("Bucket '{}' used to store database history exists", bucket);
        }
        else {
            LOGGER.info("Bucket '{}' used to store database history does not exist yet", bucket);
        }
        return bucketExists;
    }

    @Override
    public void initializeStorage() {
        LOGGER.info("Creating S3 bucket '{}' used to store database history", bucket);
        client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    }

    @Override
    public String toString() {
        return "S3";
    }
}
