/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.reactive.converters.AsSourceRecord;
import io.debezium.util.Testing;
import io.reactivex.Flowable;

public class ReactiveEngineTest extends AbstractReactiveEngineTest {
    private Flowable<DataChangeEvent<SourceRecord>> stream;

    @Before
    public void initEngine() throws IOException {
        ReactiveEngine.Builder<SourceRecord> builder = ReactiveEngine.create();
        builder
            .withConfiguration(config)
            .withOffsetCommitPolicy((noOfMsg, timeSinceLastCommit) -> false)
            .asType(AsSourceRecord.class);
        
        stream = builder.build().stream();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void consumeAllEvents() {
        stream
            .limit(EVENT_COUNT)
            .map(x -> {
                x.complete();
                return getRecordId(x.getValue());
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
            List<SourceRecord> batchEvents = stream
                    .limit(batchSize)
                    .map(x -> {
                        x.complete();
                        return x.getValue();
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
        stream
            .subscribe(x -> {
                if (cnt.incrementAndGet() == breakAt) {
                    throw new RuntimeException("Bug");
                }
                x.complete();
            }, t -> {
                Testing.print("Got error " + t.getMessage());
            }, () -> {
                Assert.fail("Pipeline should end in error");
            });
        final int restCount = EVENT_COUNT - breakAt + 1;
        List<SourceRecord> events = stream.limit(restCount).map(x -> {
            x.complete();
            return x.getValue();
        }).toList().blockingGet();
        validateRecords(events, breakAt, restCount);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void errorInConnector() {
        final int breakAt = 3;
        stream
            .doOnNext(record -> {
                if (getRecordId(record.getValue()) == breakAt) {
                    throw new RuntimeException("BUG");
                }
            })
            .subscribe(x -> {
                Testing.print(x.getValue());
                x.complete();
            }, t -> {
                Testing.print("Got error " + t.getMessage());
            }, () -> {
                Assert.fail("Pipeline should end in error");
            });
        final int restCount = EVENT_COUNT - breakAt + 1;
        List<SourceRecord> events = stream
                .limit(restCount)
                .map(x -> {
                    x.complete();
                    return x.getValue();
                })
                .toList()
                .blockingGet();
        validateRecords(events, breakAt, restCount);
    }
}