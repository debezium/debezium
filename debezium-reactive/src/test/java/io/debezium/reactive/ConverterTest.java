/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.reactivex.Flowable;

public class ConverterTest extends AbstractReactiveEngineTest {
    private Flowable<DebeziumEvent<String>> stream;

    @Before
    public void initEngine() throws IOException {

        ReactiveEngine.Builder<String> builder = ReactiveEngine.create();
        builder
            .withConfiguration(config)
            .withOffsetCommitPolicy((x, y) -> false)
            .withConverter(JsonConverter.class);
        
        stream = builder.build().stream();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void consumeAllEvents() {
        stream
            .limit(EVENT_COUNT)
            .map(x -> {
                final Document d = DocumentReader.defaultReader().read(x.getRecord()).getDocument("payload");
                x.complete();
                return (d.getInteger("batch") - 1) * BATCH_SIZE + d.getInteger("record");
            })
            .test()
            .assertComplete()
            .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
}
