/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GCSCommitLogTransferTest {
    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "test-prefix";
    private static final int RETRYABLE_ERROR_CODE = 503;
    private static final int NON_RETRYABLE_ERROR_CODE = 505;

    @Test
    public void testBucketAndPrefix() {
        String remoteDir = "gs://" + BUCKET + "/" + PREFIX;
        assertEquals(BUCKET, GCSCommitLogTransfer.getBucket(remoteDir));
        assertEquals(PREFIX, GCSCommitLogTransfer.getPrefix(remoteDir));
    }

    @Test
    public void testUploadWithRetrySuccess() throws IOException {
        Storage storageMock = mock(Storage.class);
        StorageException retryableException = TestUtils.generateGoogleStorageException(RETRYABLE_ERROR_CODE);
        when(storageMock.create(any(BlobInfo.class), any(byte[].class))).thenThrow(retryableException).thenReturn(null);
        GCSCommitLogTransfer transfer = spy(new GCSCommitLogTransfer());
        when(transfer.getStorage(any())).thenReturn(storageMock);
        CassandraConnectorConfig config = mock(CassandraConnectorConfig.class);
        when(config.remoteCommitLogRelocationDir()).thenReturn("gs://" + BUCKET + "/" + PREFIX);
        File commitLog = File.createTempFile("CommitLog-6-123", ".log");

        transfer.init(config);
        assertTrue(transfer.uploadWithRetry(commitLog, BUCKET, PREFIX));
        verify(storageMock, times(2)).create(any(BlobInfo.class), any(byte[].class));
    }

    @Test
    public void testUploadWithRetryFailure() throws IOException {
        Storage storageMock = mock(Storage.class);
        StorageException nonRetryableException = TestUtils.generateGoogleStorageException(NON_RETRYABLE_ERROR_CODE);
        when(storageMock.create(any(BlobInfo.class), any(byte[].class))).thenThrow(nonRetryableException).thenReturn(null);
        GCSCommitLogTransfer transfer = spy(new GCSCommitLogTransfer());
        when(transfer.getStorage(any())).thenReturn(storageMock);
        CassandraConnectorConfig config = mock(CassandraConnectorConfig.class);
        when(config.remoteCommitLogRelocationDir()).thenReturn("gs://" + BUCKET + "/" + PREFIX);
        File commitLog = File.createTempFile("CommitLog-6-123", ".log");

        transfer.init(config);
        assertFalse(transfer.uploadWithRetry(commitLog, BUCKET, PREFIX));
        verify(storageMock, times(1)).create(any(BlobInfo.class), any(byte[].class));
    }
}
