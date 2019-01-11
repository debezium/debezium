/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class NoOpOrderedIdBuilderTest {

    @Test
    public void shouldIncludeId() {
        assertThat(new NoOpOrderedIdBuilder().shouldIncludeId()).isFalse();
    }

    @Test
    public void buildNextId() {
        assertThat(new NoOpOrderedIdBuilder().buildNextId()).isNull();
    }

    @Test
    public void lastId() {
        NoOpOrderedIdBuilder b = new NoOpOrderedIdBuilder();
        b.buildNextId();
        assertThat(b.lastId()).isNull();
    }

    @Test
    public void shouldPersistAndRestoreState() {
        OrderedIdBuilder b = new NoOpOrderedIdBuilder();
        b.buildNextId();
        assertThat(b.lastId()).isNull();
        // doesn't throw anything
        b.restoreState(null);
    }

    @Test
    public void shouldClone() {
        CounterIdBuilder b1 = new CounterIdBuilder();
        b1.buildNextId();
        OrderedIdBuilder b2 = b1.clone();

        assertThat(b2.lastId()).isEqualTo(b1.lastId());
    }

}