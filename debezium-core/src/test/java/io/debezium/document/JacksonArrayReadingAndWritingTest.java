/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.document;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import io.debezium.util.Testing;

/**
 * @author Randall Hauch
 */
public class JacksonArrayReadingAndWritingTest implements Testing {

    private Array array;
    private Array after;
    private JacksonWriter writer = JacksonWriter.INSTANCE;
    private JacksonReader reader = JacksonReader.DEFAULT_INSTANCE;

    @Before
    public void beforeEach() {
        array = Array.create();
        after = null;
    }

    @Test
    public void shouldWriteDocumentWithSingleField() throws Exception {
        array.add("value1");
        after = reader.readArray(writer.write(array));
        assertThat(after.get(0)).isEqualTo("value1");
        assertThat(after.size()).isEqualTo(1);
    }

    @Test
    public void shouldWriteDocumentWithTwoFields() throws Exception {
        array.add("value1");
        array.add("value2");
        after = reader.readArray(writer.write(array));
        assertThat(after.get(0)).isEqualTo("value1");
        assertThat(after.get(1)).isEqualTo("value2");
        assertThat(after.size()).isEqualTo(2);
    }

    @Test
    public void shouldWriteDocumentWithNestedDocument() throws Exception {
        array.add("value1");
        array.add("value2");
        array.add(Document.create("a", "A", "b", "B"));
        after = reader.readArray(writer.write(array));
        assertThat(after.get(0)).isEqualTo("value1");
        assertThat(after.get(1)).isEqualTo("value2");
        assertThat(after.size()).isEqualTo(3);
        Document nested = after.get(2).asDocument();
        assertThat(nested.getString("a")).isEqualTo("A");
        assertThat(nested.getString("b")).isEqualTo("B");
        assertThat(nested.size()).isEqualTo(2);
    }

    @Test
    public void shouldWriteDocumentWithDeeplyNestedDocument() throws Exception {
        array.add("value1");
        array.add("value2");
        array.add(Document.create("a", "A", "b", "B", "c", Document.create("x", "X")));
        after = reader.readArray(writer.write(array));
        assertThat(after.get(0)).isEqualTo("value1");
        assertThat(after.get(1)).isEqualTo("value2");
        assertThat(after.size()).isEqualTo(3);
        Document nested = after.get(2).asDocument();
        assertThat(nested.getString("a")).isEqualTo("A");
        assertThat(nested.getString("b")).isEqualTo("B");
        assertThat(nested.size()).isEqualTo(3);
        Document deepNested = nested.getDocument("c");
        assertThat(deepNested.getString("x")).isEqualTo("X");
        assertThat(deepNested.size()).isEqualTo(1);
    }

}
