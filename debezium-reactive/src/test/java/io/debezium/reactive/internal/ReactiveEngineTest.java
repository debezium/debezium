/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive.internal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.reactive.DebeziumEvent;
import io.debezium.reactive.ReactiveEngine;
import io.debezium.util.Testing;
import io.reactivex.Flowable;

public class ReactiveEngineTest {
    private static final int EVENT_COUNT = 10;
    private static final int DEFAULT_TIMEOUT = 10_000;
    protected static final Path OFFSET_STORE_PATH = Testing.Files.createTestingPath("file-connector-offsets.txt").toAbsolutePath();

    private Flowable<DebeziumEvent> flowable;

    @Before
    public void initEngine() throws IOException {
        Files.deleteIfExists(OFFSET_STORE_PATH);
        Files.createDirectories(OFFSET_STORE_PATH.getParent());
        final Configuration config = Configuration.create()
                .with(EmbeddedEngine.ENGINE_NAME, "reactive")
                .with(EmbeddedEngine.CONNECTOR_CLASS, SimpleSourceConnector.class.getName())
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString())
                .build();

        flowable = ReactiveEngine.create()
            .using(config)
            .using((x, y) -> false)
            .build()
            .flowableWithBackPressure();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void consumeAllEvents2() {
        flowable
            .limit(EVENT_COUNT)
            .map(x -> {
                x.commit();
                return getRecordId(x.getRecord());
            })
            .test()
            .assertComplete()
            .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
}

    @Test(timeout = DEFAULT_TIMEOUT)
    public void consumeAllEventsInMultipleRuns() {
        final int batchSize = 2;
        final int batchCount = EVENT_COUNT / batchSize;

        for (int i = 0; i < batchCount; i++) {
            List<SourceRecord> batchEvents = flowable
                    .limit(batchSize)
                    .map(x -> {
                        x.commit();
                        return x.getRecord();
                    })
                    .toList()
                    .blockingGet();
            final int fromId = i * 2 + 1;
            validateRecords(batchEvents, fromId, batchSize);
        }
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void errorInConsumer() {
        final int breakAt = 3;
        AtomicInteger cnt = new AtomicInteger(0);
        flowable
            .subscribe(x -> {
                System.out.println(x.getRecord());
                if (cnt.incrementAndGet() == breakAt) {
                    throw new RuntimeException("Bug");
                }
                x.commit();
            }, t -> {
                Testing.print("Got error " + t.getMessage());
            }, () -> {
                Assert.fail("Pipeline should end in error");
            });
        final int restCount = EVENT_COUNT - breakAt + 1;
        List<SourceRecord> events = flowable.limit(restCount).map(x -> {
            x.commit();
            return x.getRecord();
        }).toList().blockingGet();
        validateRecords(events, breakAt, restCount);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void errorInConnector() {
        final int breakAt = 3;
        flowable
            .doOnNext(record -> {
                if (getRecordId(record.getRecord()) == breakAt) {
                    throw new RuntimeException("BUG");
                }
            })
            .subscribe(x -> {
                Testing.print(x.getRecord());
                x.commit();
            }, t -> {
                Testing.print("Got error " + t.getMessage());
            }, () -> {
                Assert.fail("Pipeline should end in error");
            });
        final int restCount = EVENT_COUNT - breakAt + 1;
        List<SourceRecord> events = flowable
                .limit(restCount)
                .map(x -> {
                    x.commit();
                    return x.getRecord();
                })
                .toList()
                .blockingGet();
        validateRecords(events, breakAt, restCount);
    }

    private void validateRecords(List<SourceRecord> records, int fromId, int count) {
        Assertions.assertThat(records).hasSize(count);
        int id = fromId;
        for (SourceRecord r: records) {
            validateRecord(r, id++);
        }
    }

    private void validateRecord(SourceRecord record, int expectedId) {
        Assertions.assertThat(record.key())
            .as("Correct key type").isInstanceOf(Struct.class);
        Assertions.assertThat(getRecordId(record))
            .as("Correct id").isEqualTo(expectedId);
    }

    private int getRecordId(SourceRecord record) {
        return (int)((Struct)record.key()).get("id");
    }
}
