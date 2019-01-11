/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import de.huxhorn.sulky.ulid.ULID;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;

public class UlidIdBuilderTest {

    @Test
    public void shouldAlwaysTryToBuildId() {
        UlidIdBuilder b = new UlidIdBuilder();
        assertThat(b.shouldIncludeId()).isTrue();
    }

    @Test
    public void shouldBuildIdsMonotonically() {
        UlidIdBuilder b = new UlidIdBuilder();
        String lastId = "";
        for (int i = 0; i < 100; i++) {
            String newId = b.buildNextId();
            assertThat(b.lastId()).isEqualTo(newId);
            assertThat(newId.compareTo(lastId)).isGreaterThan(0);
            lastId = newId;
        }
    }

    @Test
    public void shouldHandleOverflow() {
        UlidIdBuilder b = new UlidIdBuilder() {
            int count = 0;
            @Override
            protected Optional<ULID.Value> next() {
                if (count < 5) {
                    count++;
                    return Optional.empty();
                } else {
                    return Optional.of(new ULID.Value(0, 0));
                }
            }
        };
        long st = System.currentTimeMillis();
        char[] zeroes = new char[26];
        Arrays.fill(zeroes, '0');
        assertThat(b.buildNextId()).isEqualTo(new String(zeroes));
        assertThat(System.currentTimeMillis() - st).isGreaterThan(4);
    }

    @Test
    public void shouldPersistAndRestoreState() {
        Clock sameClock = () -> 0;
        UlidIdBuilder b1 = new UlidIdBuilder();
        b1.setClock(sameClock);
        // build id to trigger first state
        b1.buildNextId();

        String id1 = b1.buildNextId();
        String state = b1.lastId();
        UlidIdBuilder b2 = new UlidIdBuilder();
        b2.setClock(sameClock);

        b2.restoreState(state);
        assertThat(b1.lastId()).isEqualTo(b2.lastId());

        String id2 = b2.buildNextId();

        assertThat(id2.compareTo(id1)).isGreaterThan(0);
        // assert that they start with the same timestamp
        assertThat(id1.substring(0, 11)).isEqualTo(id2.substring(0, 11));
        // they should be one character different
        assertThat(id1.substring(11, id1.length() - 1)).isEqualTo(id2.substring(11, id2.length() - 1));
        assertThat(id1.charAt(id1.length() - 1) - id2.charAt(id2.length() - 1)).isEqualTo(-1);
    }

    @Test
    public void shouldClone() {
        CounterIdBuilder b1 = new CounterIdBuilder();
        b1.buildNextId();
        OrderedIdBuilder b2 = b1.clone();

        assertThat(b2.lastId()).isEqualTo(b1.lastId());
    }
}
