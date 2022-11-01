/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * @author Randall Hauch
 *
 */
public class DocumentTest {

    private Document doc;
    private Map<Path, Value> found = new LinkedHashMap<>();
    private Iterator<Map.Entry<Path, Value>> iterator;

    @Before
    public void beforeEach() {
        doc = null;
        found = new LinkedHashMap<>();
        iterator = null;
    }

    @Test
    public void shouldPerformForEachOnFlatDocument() {
        doc = Document.create("a", "A", "b", "B");
        doc.forEach((path, value) -> found.put(path, value));
        iterator = found.entrySet().iterator();
        assertPair(iterator, "/a", "A");
        assertPair(iterator, "/b", "B");
        assertNoMore(iterator);
    }

    @Test
    @FixFor("DBZ-759")
    public void shouldCreateArrayFromValues() {
        Document document = Document.create();
        document.setArray("my_field", 1, 2, 3);

        assertThat(document.has("my_field")).isTrue();
        assertThat(document.size()).isEqualTo(1);
        List<Integer> values = document.getArray("my_field")
                .streamValues()
                .map(v -> v.asInteger())
                .collect(Collectors.toList());
        assertThat(values).containsExactly(1, 2, 3);
    }

    protected void assertPair(Iterator<Map.Entry<Path, Value>> iterator, String path, Object value) {
        Map.Entry<Path, Value> entry = iterator.next();
        assertThat((Object) entry.getKey()).isEqualTo(Path.parse(path));
        assertThat(entry.getValue()).isEqualTo(Value.create(value));
    }

    protected void assertNoMore(Iterator<Map.Entry<Path, Value>> iterator) {
        assertThat(iterator.hasNext()).isFalse();
    }

}
