/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.s3;

import java.net.URISyntaxException;
import java.time.Duration;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.debezium.server.TestDatabase;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.util.Testing;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

/**
 * Integration test that verifies basic reading from PostgreSQL database and writing to s3.
 *
 * @author Ismail Simsek
 */
@QuarkusTest
public class S3IT {

    private static final int MESSAGE_COUNT = 4;
    protected static S3Client s3client = null;
    protected static TestS3Server s3server = new TestS3Server();
    protected static TestDatabase db;
    @Inject
    DebeziumServer server;
    @ConfigProperty(name = "debezium.sink.type")
    String sinkType;

    {
        Testing.Files.delete(S3TestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(S3TestConfigSource.OFFSET_STORE_PATH);
    }

    @AfterAll
    static void stop() {
        if (db != null) {
            db.stop();
        }
        if (s3server != null) {
            s3server.stop();
        }
    }

    void setupDependencies(@Observes ConnectorStartedEvent event) throws URISyntaxException {
        if (!sinkType.equals("s3")) {
            return;
        }
        db = new TestDatabase();
        db.start();
    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testS3() throws Exception {
        Testing.Print.enable();
        Assertions.assertThat(sinkType.equals("s3"));

        s3server.start();

        ProfileCredentialsProvider pcred = ProfileCredentialsProvider.create("default");
        s3client = S3Client.builder()
                .region(Region.of(S3TestConfigSource.S3_REGION))
                .credentialsProvider(pcred)
                .endpointOverride(new java.net.URI("http://localhost:" + TestS3Server.MINIO_DEFAULT_PORT_MAP))
                .build();

        s3client.createBucket(CreateBucketRequest.builder().bucket(S3TestConfigSource.S3_BUCKET).build());
        Assertions.assertThat(s3client.listBuckets().toString().contains(S3TestConfigSource.S3_BUCKET));
        s3client.createBucket(CreateBucketRequest.builder().bucket("testing-s3-slient").build());
        Assertions.assertThat(s3client.listBuckets().toString().contains("testing-s3-slient"));

        Awaitility.await().atMost(Duration.ofSeconds(S3TestConfigSource.waitForSeconds())).until(() -> {

            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(S3TestConfigSource.S3_BUCKET)
                    .build();
            ListObjectsResponse res = s3client.listObjects(listObjects);
            // List<S3Object> objects = res.contents();
            return res.contents().size() >= MESSAGE_COUNT;
        });
    }
}
