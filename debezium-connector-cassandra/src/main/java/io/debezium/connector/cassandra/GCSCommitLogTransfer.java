/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import avro.shaded.com.google.common.collect.ImmutableSet;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

public class GCSCommitLogTransfer implements CommitLogTransfer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GCSCommitLogTransfer.class);
    // see: https://cloud.google.com/storage/docs/resumable-uploads-xml#practices
    private static final Set<Integer> RETRYABLE_ERRORS = ImmutableSet.of(408, 429, 500, 502, 503, 504);
    private static final int MAX_ATTEMPTS = 10;
    private static final String FILE_CONTENT_TYPE = "text/plain";
    private static final int MAX_BACKOFF_SECONDS = 2 * 60 * 60; // 2 hours
    private static final String GCS_PREFIX = "gcs://";

    private Storage storage;
    private String bucket;
    private String prefix;

    @Override
    public void init(CassandraConnectorConfig config) throws IOException {
        storage = getStorage(config.storageCredentialKeyFile());
        bucket = getBucket(config.remoteCommitLogRelocationDir());
        prefix = getPrefix(config.remoteCommitLogRelocationDir());
    }

    @Override
    public void onSuccessTransfer(File file) {
        String archivePrefix = Paths.get(prefix, QueueProcessor.ARCHIVE_FOLDER).toString();
        if (uploadWithRetry(file, bucket, archivePrefix)) {
            CommitLogUtil.deleteCommitLog(file);
        }
    }

    @Override
    public void onErrorTransfer(File file) {
        String errorPrefix = Paths.get(prefix, QueueProcessor.ERROR_FOLDER).toString();
        if (uploadWithRetry(file, bucket, errorPrefix)) {
            CommitLogUtil.deleteCommitLog(file);
        }
    }

    Storage getStorage(String keyFile) throws IOException {
        StorageOptions options = keyFile != null
                ? StorageOptions.newBuilder().setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(keyFile))).build()
                : StorageOptions.newBuilder().setCredentials(ServiceAccountCredentials.getApplicationDefault()).build();
        return options.getService();
    }

    static String getBucket(String remoteCommitLogDir) {
        String[] bucketAndPrefix = remoteCommitLogDir.substring(GCS_PREFIX.length() - 1).split("/", 2);
        return bucketAndPrefix[0];
    }

    static String getPrefix(String remoteCommitLogDir) {
        String[] bucketAndPrefix = remoteCommitLogDir.substring(GCS_PREFIX.length() - 1).split("/", 2);
        return bucketAndPrefix[1];
    }

    boolean uploadWithRetry(File file, String bucket, String prefix) {
        ExponentialBackOff backOff = new ExponentialBackOff(MAX_BACKOFF_SECONDS);
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
            LOGGER.info("Attempting to upload file {}, try number {}", file.getName(), attempt + 1);
            try {
                uploadFile(file, bucket, prefix);
                LOGGER.info("Successfully uploaded file to url gs://{}/{}/{}", bucket, prefix, file.getName());
                return true;
            } catch (Exception e) {
                if (isRetryable(e)) {
                    LOGGER.warn("Uploading {} failed, retrying in {}s", file.getName(), backOff.getBackoffMs() / 1000);
                    try {
                        backOff.doWait();
                    } catch (InterruptedException ie) {
                        LOGGER.error("Thread was interrupted before the following file could be retried: " + file.getName(), ie);
                        return false;
                    }
                } else {
                    LOGGER.error("Could not upload file {}: ", file.getName(), e);
                    return false;
                }
            }
        }
        LOGGER.error("Exhausted attempts to upload {} after {} tries", file.getName(), MAX_ATTEMPTS);
        return false;
    }

    private void uploadFile(File file, String bucket, String prefix) throws IOException {
        byte[] content = Files.readAllBytes(file.toPath());
        String fullPathFileName = Paths.get(prefix, file.getName()).toString();
        BlobId blobId = BlobId.of(bucket, fullPathFileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(FILE_CONTENT_TYPE).build();
        storage.create(blobInfo, content);
    }

    private static boolean isRetryable(Exception e) {
        if (e instanceof StorageException) {
            Throwable cause = e.getCause();
            if (cause instanceof GoogleJsonResponseException) {
                GoogleJsonResponseException inner = (GoogleJsonResponseException) cause;
                if (inner.getDetails() != null) {
                    int code = inner.getDetails().getCode();
                    return RETRYABLE_ERRORS.contains(code);
                }
            }
        }
        return false;
    }
}
