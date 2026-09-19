/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.s3;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.spi.storage.OversizedRecord;
import io.debezium.spi.storage.OversizedRecordReference;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

class S3OversizedRecordStorageTest {

    private S3Client s3Client;
    private S3OversizedRecordStorage storage;

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        storage = spy(new S3OversizedRecordStorage());
        doReturn(s3Client).when(storage).createS3Client(any(), any(), any());
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    @Test
    void shouldSynchronouslyStoreThePayloadAtTheConfiguredBasePath() throws Exception {
        storage.configure(configuration());
        byte[] payload = "complete source record".getBytes(StandardCharsets.UTF_8);
        OversizedRecord record = new OversizedRecord(
                "inventory.customers/offset-123-sha256-abc.json",
                payload,
                "application/json");

        OversizedRecordReference reference = storage.write(record);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());
        PutObjectRequest request = requestCaptor.getValue();
        assertEquals("claim-check-bucket", request.bucket());
        assertEquals("debezium/oversized/inventory.customers/offset-123-sha256-abc.json", request.key());
        assertEquals("application/json", request.contentType());
        assertEquals(payload.length, request.contentLength());
        assertArrayEquals(payload, bodyCaptor.getValue().contentStreamProvider().newStream().readAllBytes());

        assertEquals("s3", reference.storage());
        assertEquals(
                URI.create("s3://claim-check-bucket/debezium/oversized/inventory.customers/offset-123-sha256-abc.json"),
                reference.uri());
        assertEquals(payload.length, reference.sizeBytes());
        ArgumentCaptor<AwsCredentialsProvider> credentialsProviderCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(storage).createS3Client(any(), org.mockito.ArgumentMatchers.eq(Region.US_WEST_1), credentialsProviderCaptor.capture());
        assertTrue(credentialsProviderCaptor.getValue() instanceof DefaultCredentialsProvider);
    }

    @Test
    void shouldUseConfiguredStaticCredentials() {
        Map<String, Object> properties = new LinkedHashMap<>(configurationProperties());
        properties.put(S3OversizedRecordStorage.ACCESS_KEY_ID_CONFIG, "access-key-id");
        properties.put(S3OversizedRecordStorage.SECRET_ACCESS_KEY_CONFIG, "secret-access-key");

        storage.configure(Configuration.from(properties));

        ArgumentCaptor<AwsCredentialsProvider> credentialsProviderCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(storage).createS3Client(any(), org.mockito.ArgumentMatchers.eq(Region.US_WEST_1), credentialsProviderCaptor.capture());
        assertTrue(credentialsProviderCaptor.getValue() instanceof StaticCredentialsProvider);
        assertEquals("access-key-id", credentialsProviderCaptor.getValue().resolveCredentials().accessKeyId());
        assertEquals("secret-access-key", credentialsProviderCaptor.getValue().resolveCredentials().secretAccessKey());
    }

    @Test
    void shouldUseDefaultCredentialsWhenStaticCredentialsAreIncomplete() {
        Map<String, Object> properties = new LinkedHashMap<>(configurationProperties());
        properties.put(S3OversizedRecordStorage.ACCESS_KEY_ID_CONFIG, "access-key-id");

        storage.configure(Configuration.from(properties));

        ArgumentCaptor<AwsCredentialsProvider> credentialsProviderCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(storage).createS3Client(any(), org.mockito.ArgumentMatchers.eq(Region.US_WEST_1), credentialsProviderCaptor.capture());
        assertTrue(credentialsProviderCaptor.getValue() instanceof DefaultCredentialsProvider);
    }

    @Test
    void shouldPropagateUploadFailureWithoutReturningAReference() {
        storage.configure(configuration());
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new IllegalStateException("S3 unavailable"));
        OversizedRecord record = new OversizedRecord("record.json", new byte[]{ 1 }, "application/json");

        DebeziumException exception = assertThrows(DebeziumException.class, () -> storage.write(record));
        assertTrue(exception.getMessage().contains("Failed to store oversized record"));
        assertTrue(exception.getMessage().contains("s3://claim-check-bucket/debezium/oversized/record.json"));
        assertEquals("S3 unavailable", exception.getCause().getMessage());
    }

    @Test
    void shouldRequireAnS3BasePath() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(S3OversizedRecordStorage.BASE_PATH_CONFIG, "https://claim-check-bucket/prefix");
        properties.put(S3OversizedRecordStorage.REGION_CONFIG, "us-west-1");

        DebeziumException exception = assertThrows(
                DebeziumException.class,
                () -> storage.configure(Configuration.from(properties)));
        assertTrue(exception.getMessage().contains("must be an S3 URI"));
    }

    @Test
    void shouldRejectCredentialsInTheBasePath() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(S3OversizedRecordStorage.BASE_PATH_CONFIG, "s3://user@claim-check-bucket/prefix");
        properties.put(S3OversizedRecordStorage.REGION_CONFIG, "us-west-1");

        DebeziumException exception = assertThrows(
                DebeziumException.class,
                () -> storage.configure(Configuration.from(properties)));
        assertTrue(exception.getMessage().contains("must be an S3 URI"));
    }

    @Test
    void shouldRequireARegion() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(S3OversizedRecordStorage.BASE_PATH_CONFIG, "s3://claim-check-bucket/prefix");

        DebeziumException exception = assertThrows(
                DebeziumException.class,
                () -> storage.configure(Configuration.from(properties)));
        assertTrue(exception.getMessage().contains(S3OversizedRecordStorage.REGION_CONFIG));
    }

    @Test
    void shouldRejectWritesBeforeConfiguration() {
        OversizedRecord record = new OversizedRecord("record.json", new byte[]{ 1 }, "application/json");

        DebeziumException exception = assertThrows(DebeziumException.class, () -> storage.write(record));
        assertTrue(exception.getMessage().contains("not configured"));
    }

    @Test
    void shouldCloseTheClient() {
        storage.configure(configuration());

        storage.close();

        verify(s3Client).close();
    }

    private static Configuration configuration() {
        return Configuration.from(configurationProperties());
    }

    private static Map<String, Object> configurationProperties() {
        return Map.of(
                S3OversizedRecordStorage.BASE_PATH_CONFIG, "s3://claim-check-bucket/debezium/oversized",
                S3OversizedRecordStorage.REGION_CONFIG, "us-west-1");
    }
}
