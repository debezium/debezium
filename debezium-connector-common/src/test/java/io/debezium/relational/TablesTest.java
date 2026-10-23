/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.Tables.TableFilter;

public class TablesTest {

    /**
     * Table ids are equal when their string representations are, so a table in catalog {@code s1} equals one
     * in schema {@code s1}. A filter may still include only one of them, and so must a filter caching it,
     * whichever of the two it is asked about first.
     */
    @Test
    @FixFor("dbz#1563")
    public void cachedFilterShouldTellApartEqualTableIdsOfDifferentParts() {
        final TableId inCatalog = new TableId("s1", null, "tab2");
        final TableId inSchema = new TableId(null, "s1", "tab2");
        assertThat(inCatalog).isEqualTo(inSchema);

        final TableFilter filter = TableFilter.cached(tableId -> "s1".equals(tableId.schema()));

        assertThat(filter.isIncluded(inCatalog)).isFalse();
        assertThat(filter.isIncluded(inSchema)).isTrue();
    }
}
