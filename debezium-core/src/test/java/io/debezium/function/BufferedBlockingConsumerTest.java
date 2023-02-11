/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Randall Hauch
 */
public class BufferedBlockingConsumerTest {

    private List<Integer> history;
    private BlockingConsumer<Integer> consumer;

    @Before
    public void beforeEach() {
        history = new LinkedList<>();
        consumer = history::add;
    }

    @Test
    public void shouldMaintainSameOrder() throws InterruptedException {
        BufferedBlockingConsumer<Integer> buffered = BufferedBlockingConsumer.bufferLast(consumer);

        // Add several values ...
        buffered.accept(1);
        buffered.accept(2);
        buffered.accept(3);
        buffered.accept(4);
        buffered.accept(5);

        // And verify the history contains all but the last value ...
        assertThat(history).containsExactly(1, 2, 3, 4);

        // Flush the last value...
        buffered.close(i -> i);

        // And verify the history contains the same values ...
        assertThat(history).containsExactly(1, 2, 3, 4, 5);
    }

}
