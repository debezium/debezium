/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class CounterIdBuilderTest {
    @Test
    public void shouldAlwaysTryToBuildId() {
        CounterIdBuilder b = new CounterIdBuilder();
        assertThat(b.shouldIncludeId()).isTrue();
    }

    @Test
    public void shouldBuildIdsMonotonically() {
        CounterIdBuilder b = new CounterIdBuilder();
        long lastId = 0L;
        for (int i = 0; i < 100; i++) {
            long newId = Long.parseLong(b.buildNextId());
            assertThat(Long.parseLong(b.lastId())).isEqualTo(newId);
            assertThat(newId).isEqualTo(lastId + 1);
            lastId = newId;
        }
    }

    @Test
    public void shouldPersistAndRestoreState() {
        CounterIdBuilder b1 = new CounterIdBuilder();
        b1.buildNextId();
        String state = b1.lastId();
        CounterIdBuilder b2 = new CounterIdBuilder();

        b2.restoreState(state);
        assertThat(b2.lastId()).isEqualTo(b1.lastId());
        assertThat(b1.buildNextId()).isEqualTo(b2.buildNextId());
    }

    @Test
    public void shouldClone() {
        CounterIdBuilder b1 = new CounterIdBuilder();
        b1.buildNextId();
        OrderedIdBuilder b2 = b1.clone();

        assertThat(b2.lastId()).isEqualTo(b1.lastId());
    }

}