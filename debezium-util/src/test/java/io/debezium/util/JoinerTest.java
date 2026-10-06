/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit test for {@link Joiner}.
 *
 * @author Randall Hauch
 */
class JoinerTest {

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinArrayAndIgnoreNulls() {
        final var joiner = Joiner.on(",");
        assertThat(joiner.join(new Object[]{ "a", null, "b", "c" })).isEqualTo("a,b,c");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinVarargsAndIgnoreNulls() {
        final var joiner = Joiner.on(",");
        assertThat(joiner.join("a", null, "b", "c")).isEqualTo("a,b,c");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinIterableAndIgnoreNulls() {
        final var joiner = Joiner.on(",");
        final List<String> values = Arrays.asList("a", null, "b", "c");
        assertThat(joiner.join(values)).isEqualTo("a,b,c");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinIterableWithNextAndAdditionalValues() {
        final var joiner = Joiner.on(",");
        final List<String> values = Arrays.asList("a", "b");
        assertThat(joiner.join(values, "c", null, "d")).isEqualTo("a,b,c,d");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinIteratorAndIgnoreNulls() {
        final var joiner = Joiner.on(",");
        final List<String> values = Arrays.asList("a", null, "b");
        assertThat(joiner.join(values.iterator())).isEqualTo("a,b");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinWithPrefixAndDelimiter() {
        final var joiner = Joiner.on("/", "/");
        assertThat(joiner.join("a", "b", "c")).isEqualTo("/a/b/c");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldJoinWithPrefixDelimiterAndSuffix() {
        final var joiner = Joiner.on("[", ",", "]");
        assertThat(joiner.join("a", "b", "c")).isEqualTo("[a,b,c]");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldReturnEmptyOrPrefixSuffixWhenEmpty() {
        assertThat(Joiner.on(",").join(Collections.emptyList())).isEqualTo("");
        assertThat(Joiner.on("[", ",", "]").join(Collections.emptyList())).isEqualTo("[]");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldBeReusableAcrossMultipleInvocationsWithoutAccumulatingState() {
        final var joiner = Joiner.on(",");
        assertThat(joiner.join("a", "b")).isEqualTo("a,b");
        assertThat(joiner.join("c", "d")).isEqualTo("c,d");
        assertThat(joiner.join(new Object[]{ "x", "y" })).isEqualTo("x,y");
        assertThat(joiner.join(List.of("1", "2"))).isEqualTo("1,2");
        assertThat(joiner.join(List.of("first"), "second", "third")).isEqualTo("first,second,third");
        assertThat(joiner.join(List.of("iter1", "iter2").iterator())).isEqualTo("iter1,iter2");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldBeReusableWithPrefixAndSuffix() {
        final var joiner = Joiner.on("[", ",", "]");
        assertThat(joiner.join("a", "b")).isEqualTo("[a,b]");
        assertThat(joiner.join("c", "d")).isEqualTo("[c,d]");
    }

    @Test
    @FixFor("debezium/dbz#2802")
    void shouldBeThreadSafeAcrossConcurrentInvocations() throws Exception {
        final var joiner = Joiner.on(",");
        final var executor = Executors.newFixedThreadPool(4);
        try {
            final List<Callable<Boolean>> tasks = List.of(
                    () -> "a,b".equals(joiner.join("a", "b")),
                    () -> "c,d".equals(joiner.join("c", "d")),
                    () -> "e,f".equals(joiner.join("e", "f")),
                    () -> "g,h".equals(joiner.join("g", "h")));
            final List<Future<Boolean>> futures = executor.invokeAll(tasks);
            for (final var future : futures) {
                assertThat(future.get()).isTrue();
            }
        }
        finally {
            executor.shutdown();
        }
    }
}
