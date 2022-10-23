/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.history;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.http.entity.ContentType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.storage.s3.history.S3SchemaHistory;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3DatabaseHistoryTest extends AbstractSchemaHistoryTest {
    final public static String IMAGE_TAG = "2.4.14";
    final public static String BUCKET = "debezium";

    final public static String OBJECT_NAME = String.format("db-history-%s.log", Thread.currentThread().getName());

    final private static S3MockContainer container = new S3MockContainer(IMAGE_TAG);
    private static S3Client client;

    @BeforeClass
    public static void startS3() {
        container.start();
        client = S3Client.builder()
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .region(Region.AWS_GLOBAL)
                .endpointOverride(URI.create(container.getHttpEndpoint())).build();
    }

    @AfterClass
    public static void stopS3() {
        container.stop();
    }

    @Override
    public void afterEach() {
        client.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(OBJECT_NAME).build());
        client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET).build());
        super.afterEach();
    }

    @Override
    protected SchemaHistory createHistory() {
        SchemaHistory history = new S3SchemaHistory();
        Configuration config = Configuration.create()
                .with(S3SchemaHistory.ACCESS_KEY_ID, "")
                .with(S3SchemaHistory.SECRET_ACCESS_KEY, "")
                .with(S3SchemaHistory.BUCKET_CONFIG, BUCKET)
                .with(S3SchemaHistory.REGION_CONFIG, Region.AWS_GLOBAL)
                .with(S3SchemaHistory.ENDPOINT_CONFIG, container.getHttpEndpoint())
                .build();
        history.configure(config, null, SchemaHistoryListener.NOOP, true);
        history.start();
        return history;
    }

    @Test
    public void InitializeStorageShouldCreateBucket() {
        client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET).build());
        history.initializeStorage();

        Assert.assertTrue(client.listBuckets().buckets().stream().map(Bucket::name).collect(Collectors.toList()).contains(BUCKET));
    }

    @Test
    public void StoreRecordShouldSaveRecordsInS3() throws IOException {
        record(01, 0, "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", all, t3, t2, t1, t0);
        List<S3Object> s3ObjectList = client.listObjects(ListObjectsRequest.builder().bucket(BUCKET).build()).contents();
        Assert.assertEquals(1, s3ObjectList.size());

        S3Object s3Object = s3ObjectList.get(0);
        Assert.assertEquals(OBJECT_NAME, s3Object.key());

        InputStream objectInputStream = client.getObject(
                GetObjectRequest.builder().bucket(BUCKET)
                        .key(OBJECT_NAME)
                        .responseCacheControl(ContentType.TEXT_PLAIN.getMimeType())
                        .build(),
                ResponseTransformer.toInputStream());
        BufferedReader historyReader = new BufferedReader(new InputStreamReader(objectInputStream, StandardCharsets.UTF_8));
        DocumentReader reader = DocumentReader.defaultReader();
        List<HistoryRecord> historyRecords = new ArrayList<>();
        while (true) {
            String line = historyReader.readLine();
            if (line == null) {
                break;
            }
            historyRecords.add(new HistoryRecord(reader.read(historyReader.readLine())));
        }

        Assert.assertEquals(1, historyRecords.size());
        Assert.assertEquals("CREATE TABLE foo ( first VARCHAR(22) NOT NULL );", historyRecords.get(0).ddl());
        Assert.assertEquals(1, historyRecords.get(0).position().getInteger("position").intValue());
        Assert.assertEquals(0, historyRecords.get(0).position().getInteger("entry").intValue());
    }
}
