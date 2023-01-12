/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3.history;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.util.FunctionalReadWriteLock;

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
public class S3SchemaHistory extends AbstractSchemaHistory {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3SchemaHistory.class);
    public static final String ACCESS_KEY_ID_CONFIG = "s3.access.key.id";
    public static final String SECRET_ACCESS_KEY_CONFIG = "s3.secret.access.key";
    public static final String REGION_CONFIG = CONFIGURATION_FIELD_PREFIX_STRING + "s3.region.name";
    public static final String BUCKET_CONFIG = CONFIGURATION_FIELD_PREFIX_STRING + "s3.bucket.name";
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
            .withImportance(Importance.MEDIUM);

    public static final Field BUCKET = Field.create(BUCKET_CONFIG)
            .withDisplayName("S3 bucket")
            .withType(Type.STRING)
            .withImportance(Importance.HIGH);

    public static final Field ENDPOINT = Field.create(ENDPOINT_CONFIG)
            .withDisplayName("S3 endpoint")
            .withType(Type.STRING)
            .withImportance(Importance.LOW);

    public static final Field.Set ALL_FIELDS = Field.setOf(ACCESS_KEY_ID, SECRET_ACCESS_KEY, REGION, BUCKET, ENDPOINT);

    private final AtomicBoolean running = new AtomicBoolean();
    private final FunctionalReadWriteLock lock = FunctionalReadWriteLock.reentrant();
    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();
    private final String objectName = String.format("db-history-%s.log", Thread.currentThread().getName());

    private String bucket = null;
    private Region region = null;
    private URI endpoint = null;
    private AwsCredentialsProvider credentialsProvider = null;

    private volatile S3Client client = null;
    private volatile List<HistoryRecord> records = new ArrayList<>();

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        if (!config.validateAndRecord(ALL_FIELDS, LOGGER::error)) {
            throw new SchemaHistoryException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        bucket = config.getString(BUCKET);
        if (bucket == null) {
            throw new DebeziumException(BUCKET + " is required to be set");
        }

        // Unknown value is mapped to aws-global internally
        final var regionName = config.getString(REGION);
        if (regionName == null) {
            throw new DebeziumException(REGION + " is required to be set");
        }
        region = Region.of(regionName);

        final var uriString = config.getString(ENDPOINT);
        if (uriString != null) {
            LOGGER.info("Using explicitly configured endpoint " + uriString);
            endpoint = URI.create(uriString);
        }

        if (config.getString(ACCESS_KEY_ID) == null && config.getString(SECRET_ACCESS_KEY) == null) {
            LOGGER.info("DefaultCreadentialsProvider is used for authentication");
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        else {
            LOGGER.info("StaticCredentialsProvider is used for authentication");
            AwsCredentials credentials = AwsBasicCredentials.create(config.getString(ACCESS_KEY_ID), config.getString(SECRET_ACCESS_KEY));
            credentialsProvider = StaticCredentialsProvider.create(credentials);
        }

    }

    @Override
    public synchronized void start() {
        if (client == null) {
            S3ClientBuilder clientBuilder = S3Client.builder().credentialsProvider(credentialsProvider)
                    .region(region);
            if (endpoint != null) {
                clientBuilder.endpointOverride(endpoint);
            }

            client = clientBuilder.build();
        }

        lock.write(() -> {
            if (running.compareAndSet(false, true)) {
                if (!storageExists()) {
                    initializeStorage();
                }

                InputStream objectInputStream = null;
                try {
                    GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(objectName).responseContentType(OBJECT_CONTENT_TYPE).build();
                    objectInputStream = client.getObject(request, ResponseTransformer.toInputStream());
                }
                catch (NoSuchKeyException e) {
                    // do nothing
                }
                catch (S3Exception e) {
                    throw new SchemaHistoryException("Can not retrieve history object from S3", e);
                }

                if (objectInputStream != null) {
                    try (BufferedReader historyReader = new BufferedReader(new InputStreamReader(objectInputStream, StandardCharsets.UTF_8))) {
                        while (true) {
                            String line = historyReader.readLine();
                            if (line == null) {
                                break;
                            }
                            if (!line.isEmpty()) {
                                records.add(new HistoryRecord(reader.read(line)));
                            }
                        }
                    }
                    catch (IOException e) {
                        throw new SchemaHistoryException("Unable to read object content", e);
                    }
                }
            }
        });
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            if (client != null) {
                client.close();
            }
        }

        super.stop();
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws SchemaHistoryException {
        if (client == null) {
            throw new IllegalStateException("No S3 client is available. Ensure that 'start()' is called before storing database history records.");
        }
        if (record == null) {
            return;
        }

        LOGGER.trace("Storing record into database history: {}", record);
        lock.write(() -> {
            if (!running.get()) {
                throw new IllegalStateException("The history has been stopped and will not accept more records");
            }

            records.add(record);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (BufferedWriter historyWriter = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
                records.forEach(r -> {
                    String line = null;
                    try {
                        line = writer.write(r.document());
                    }
                    catch (IOException e) {
                        LOGGER.error("Failed to convert record to string: {}", r, e);
                    }

                    if (line != null) {
                        try {
                            historyWriter.newLine();
                            historyWriter.append(line);
                        }
                        catch (IOException e) {
                            LOGGER.error("Failed to add record {} to history", r, e);
                            return;
                        }
                    }
                });
            }
            catch (IOException e) {
                LOGGER.error("Failed to convert record to string: {}", record, e);
            }

            try {
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectName)
                        .contentType(OBJECT_CONTENT_TYPE)
                        .build();
                client.putObject(request, RequestBody.fromBytes(outputStream.toByteArray()));
            }
            catch (S3Exception e) {
                throw new SchemaHistoryException("can not store record to S3", e);
            }

        });
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        lock.write(() -> this.records.forEach(records));
    }

    @Override
    public boolean exists() {
        return !records.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return client.listBuckets().buckets().stream().map(Bucket::name).collect(Collectors.toList()).contains(config.getString(bucket));
    }

    @Override
    public void initializeStorage() {
        client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    }

    @Override
    public String toString() {
        return "S3";
    }
}
