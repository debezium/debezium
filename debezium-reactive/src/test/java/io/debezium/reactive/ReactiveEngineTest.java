/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.reactive;

import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.Test;

public class ReactiveEngineTest {

    @Test
    public void consumeAllEvents() {
        ReactiveEngine.Builder<SourceRecord> builder = ReactiveEngine.create();
        Assertions.assertThat(builder).isNull();
    }
}
