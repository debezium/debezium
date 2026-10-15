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
    private final TableFilter filter = TableFilter.cached(id -> {
        evaluations.incrementAndGet();
        return id.table().startsWith("in");
    });

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
    public void shouldBoundExcludedDecisionsAndKeepIncludedOnes() {
        final TableId included = new TableId("db", null, "in1");
        final TableId firstExcluded = new TableId("db", null, "out0");
        filter.isIncluded(included);
        filter.isIncluded(firstExcluded);

        for (int i = 1; i < 20_000; i++) {
            filter.isIncluded(new TableId("db", null, "out" + i));
        }
        final int before = evaluations.get();

        assertThat(filter.isIncluded(included)).isTrue();
        assertThat(evaluations).hasValue(before);
        assertThat(filter.isIncluded(firstExcluded)).isFalse();
        assertThat(evaluations).hasValue(before + 1);
    }
}
