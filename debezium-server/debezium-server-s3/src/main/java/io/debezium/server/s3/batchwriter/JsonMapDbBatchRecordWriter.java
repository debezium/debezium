/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.server.s3.batchwriter;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.debezium.server.s3.objectkeymapper.ObjectKeyMapper;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class JsonMapDbBatchRecordWriter implements BatchRecordWriter, AutoCloseable {

    static final File TEMPDIR = Files.createTempDir();
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonMapDbBatchRecordWriter.class);
    private static final LocalDateTime batchTime = LocalDateTime.now();

    @ConfigProperty(name = "debezium.sink.s3.s3batch.maxeventsperbatch")
    private static int MAX_ROWS;
    private final S3Client s3Client;
    private final String bucket;
    private final ObjectKeyMapper objectKeyMapper;
    DB cdcDb;
    ConcurrentMap<String, String> map_data;
    ConcurrentMap<String, Integer> map_batchid;

    public JsonMapDbBatchRecordWriter(ObjectKeyMapper mapper, S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.objectKeyMapper = mapper;

        // init db
        this.cdcDb = DBMaker
                .fileDB(TEMPDIR.toPath().resolve("debeziumevents.db").toFile())
                .fileMmapEnable()
                .make();
        map_data = cdcDb
                .hashMap("map_data", Serializer.STRING, Serializer.STRING)
                .createOrOpen();
        map_batchid = cdcDb
                .hashMap("map_batchid", Serializer.STRING, Serializer.INTEGER)
                .createOrOpen();

    }

    @Override
    public void append(String destination, String eventValue) throws IOException {

        if (!map_data.containsKey(destination)) {
            LOGGER.debug("Creting new batch {}", destination);
            map_data.put(destination, eventValue);
            // dont change if its new batch
            map_batchid.putIfAbsent(destination, 0);
        }
        else {
            map_data.put(destination, map_data.get(destination) + IOUtils.LINE_SEPARATOR + eventValue);
        }
        if (StringUtils.countMatches(map_data.get(destination), IOUtils.LINE_SEPARATOR) >= MAX_ROWS) {
            this.uploadBatchFile(destination);
        }
        cdcDb.commit();
    }

    private void uploadBatchFile(String destination) {
        Integer batchId = map_batchid.get(destination);
        String s3File = objectKeyMapper.map(destination, batchTime, batchId);
        final PutObjectRequest putRecord = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3File)
                .build();
        s3Client.putObject(putRecord, RequestBody.fromString(map_data.get(destination)));
        // increment batch id
        map_batchid.put(destination, batchId + 1);
        // start new batch
        map_data.remove(destination);
        cdcDb.commit();
    }

    @Override
    public void uploadBatch() throws IOException {
        for (String k : map_data.keySet()) {
            uploadBatchFile(k);
            map_data.remove(k);
            map_batchid.remove(k);
            cdcDb.commit();
        }
    }

    @Override
    public void close() throws IOException {
        this.uploadBatch();
        cdcDb.close();
    }
}
