/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.debezium.server.s3.batchwriter;

import com.google.common.io.Files;
import io.debezium.server.s3.objectkeymapper.ObjectKeyMapper;
import org.apache.commons.io.FileUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JsonFileBatchRecordWriter implements BatchRecordWriter, AutoCloseable {
    static final ConcurrentHashMap<String, BatchFile> files = new ConcurrentHashMap<>();
    static final File TEMPDIR = Files.createTempDir();
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonFileBatchRecordWriter.class);
    private static final LocalDateTime batchTime = LocalDateTime.now();
    @ConfigProperty(name = "debezium.sink.s3.s3batch.maxeventsperbatch")
    private static int MAX_ROWS;
    private final S3Client s3Client;
    private final String bucket;
    private final ObjectKeyMapper mapper;

    public JsonFileBatchRecordWriter(ObjectKeyMapper mapper, S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.mapper = mapper;
    }

    @Override
    public void append(String destination, String eventValue) throws IOException {

        if (!files.containsKey(destination)) {
            File newBatchFileName = TEMPDIR.toPath().resolve(mapper.map(destination, batchTime, 0)).toFile();
            LOGGER.debug("Creting new Batch {} File {}", 0, newBatchFileName.getAbsolutePath());
            files.put(destination, new BatchFile(newBatchFileName));
        }
        BatchFile afile = files.get(destination);
        afile.append(eventValue);
        // process batch
        if (afile.getNumRecords() > MAX_ROWS) {
            this.uploadBatchFile(afile.getAbsolutePath());
            File newBatchFileName = TEMPDIR.toPath().resolve(mapper.map(destination, batchTime, afile.getBatchId())).toFile();
            LOGGER.debug("Creting new Batch {} File {}", afile.getBatchId(), newBatchFileName.getAbsolutePath());
            afile.setBatchFile(newBatchFileName);
        }

    }

    private void uploadBatchFile(Path file) {
        LOGGER.debug("Uploading file {} to s3 {}", file.toAbsolutePath(), bucket);
        final PutObjectRequest putRecord = PutObjectRequest.builder()
                .bucket(bucket)
                .key(TEMPDIR.toPath().relativize(file).toString())
                .build();
        s3Client.putObject(putRecord, file);
        LOGGER.debug("Deleting File {}", file.toAbsolutePath());
        file.toFile().delete();
    }

    @Override
    public void uploadBatch() throws IOException {
        for (Map.Entry<String, BatchFile> o : files.entrySet()) {
            o.getValue().close();
            uploadBatchFile(o.getValue().getAbsolutePath());
        }
        files.clear();
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, BatchFile> o : files.entrySet()) {
            o.getValue().close();
        }
        FileUtils.deleteDirectory(TEMPDIR);
    }
}
