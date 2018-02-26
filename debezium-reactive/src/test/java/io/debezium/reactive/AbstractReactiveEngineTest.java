/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Before;

import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.util.Testing;

public abstract class AbstractReactiveEngineTest {
    protected static final int BATCH_COUNT = 2;
    protected static final int BATCH_SIZE = 5;
    protected static final int EVENT_COUNT = 10;
    protected static final int DEFAULT_TIMEOUT = 10_000;
    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();

    protected Configuration config;

    @Before
    public void init() throws IOException {
        Files.deleteIfExists(OFFSET_STORE_PATH);
        Files.createDirectories(OFFSET_STORE_PATH.getParent());
        config = Configuration.create()
                .with(EmbeddedEngine.ENGINE_NAME, "reactive")
                .with(EmbeddedEngine.CONNECTOR_CLASS, SimpleSourceConnector.class.getName())
                .with(SimpleSourceConnector.RECORD_COUNT_PER_BATCH, BATCH_SIZE)
                .with(SimpleSourceConnector.BATCH_COUNT, BATCH_COUNT)
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString())
                .build();
    }

    protected void validateRecords(List<SourceRecord> records, int fromId, int count) {
        Assertions.assertThat(records).hasSize(count);
        int id = fromId;
        for (SourceRecord r: records) {
            validateRecord(r, id++);
        }
    }

    protected void validateRecord(SourceRecord record, int expectedId) {
        Assertions.assertThat(record.key())
            .as("Correct key type").isInstanceOf(Struct.class);
        Assertions.assertThat(getRecordId(record))
            .as("Correct id").isEqualTo(expectedId);
    }

    protected int getRecordId(SourceRecord record) {
        return (int)((Struct)record.key()).get("id");
    }}
