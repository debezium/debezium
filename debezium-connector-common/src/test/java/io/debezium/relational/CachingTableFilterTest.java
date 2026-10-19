/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Tables.TableFilter;

public class CachingTableFilterTest {

    private final AtomicInteger evaluations = new AtomicInteger();
    private static final int MAX_EXCLUDED = 100;

    private final TableFilter filter = TableFilter.cached(id -> {
        evaluations.incrementAndGet();
        return id.table().startsWith("in");
    }, MAX_EXCLUDED);

    @Test

    @FixFor("debezium/dbz#2874")
    public void shouldEvaluateEachTableOnce() {
        final TableId included = new TableId("db", null, "in1");
        final TableId excluded = new TableId("db", null, "out1");

        for (int i = 0; i < 3; i++) {
            assertThat(filter.isIncluded(included)).isTrue();
            assertThat(filter.isIncluded(excluded)).isFalse();
        }

        assertThat(evaluations).hasValue(2);
    }

    @Test
    @FixFor("debezium/dbz#2874")
    public void shouldEvictTheLeastRecentlyUsedExcludedDecisionsAndKeepIncludedOnes() {
        final TableId included = new TableId("db", null, "in1");
        final TableId untouched = new TableId("db", null, "out0");
        final TableId newest = new TableId("db", null, "out" + (2 * MAX_EXCLUDED - 1));
        filter.isIncluded(included);
        filter.isIncluded(untouched);

        for (int i = 1; i < 2 * MAX_EXCLUDED; i++) {
            filter.isIncluded(new TableId("db", null, "out" + i));
        }
        final int before = evaluations.get();

        assertThat(filter.isIncluded(included)).isTrue();
        assertThat(filter.isIncluded(newest)).isFalse();
        assertThat(evaluations).hasValue(before);
        assertThat(filter.isIncluded(untouched)).isFalse();
        assertThat(evaluations).hasValue(before + 1);
    }

    @Test
    @FixFor("debezium/dbz#2874")
    public void shouldKeepAnExcludedDecisionThatIsAskedForAgain() {
        final TableId hot = new TableId("db", null, "hot");
        final TableId cold = new TableId("db", null, "cold");
        filter.isIncluded(hot);
        filter.isIncluded(cold);

        for (int i = 0; i < 2 * MAX_EXCLUDED; i++) {
            filter.isIncluded(new TableId("db", null, "out" + i));
            filter.isIncluded(hot);
        }
        final int before = evaluations.get();

        assertThat(filter.isIncluded(hot)).isFalse();
        assertThat(evaluations).hasValue(before);
        assertThat(filter.isIncluded(cold)).isFalse();
        assertThat(evaluations).hasValue(before + 1);
    }

    @Test
    @FixFor("debezium/dbz#2874")
    public void shouldKeepEveryExcludedDecisionWhenNotBounded() {
        final AtomicInteger calls = new AtomicInteger();
        final TableFilter unbounded = TableFilter.cached(id -> {
            calls.incrementAndGet();
            return false;
        }, 0);

        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < 3 * MAX_EXCLUDED; i++) {
                unbounded.isIncluded(new TableId("db", null, "out" + i));
            }
        }

        assertThat(calls).hasValue(3 * MAX_EXCLUDED);
    }
}
