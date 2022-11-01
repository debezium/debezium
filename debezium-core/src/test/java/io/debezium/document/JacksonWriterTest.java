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
public class JacksonWriterTest implements Testing {

    private Document doc;
    private Document after;
    private JacksonWriter writer = JacksonWriter.INSTANCE;
    private JacksonReader reader = JacksonReader.DEFAULT_INSTANCE;

    @Before
    public void beforeEach() {
        doc = Document.create();
        after = null;
    }

    @Test
    public void shouldWriteDocumentWithSingleField() throws Exception {
        doc.set("field", "value");
        after = reader.read(writer.write(doc));
        assertThat(after.getString("field")).isEqualTo("value");
        assertThat(after.size()).isEqualTo(1);
    }

    @Test
    public void shouldWriteDocumentWithTwoFields() throws Exception {
        doc.set("field1", "value");
        doc.set("field2", 22);
        after = reader.read(writer.write(doc));
        assertThat(after.getString("field1")).isEqualTo("value");
        assertThat(after.getInteger("field2")).isEqualTo(22);
        assertThat(after.size()).isEqualTo(2);
    }

    @Test
    public void shouldWriteDocumentWithNestedDocument() throws Exception {
        doc.set("field1", "value");
        doc.set("field2", 22);
        doc.set("field3", Document.create("a", "A", "b", "B"));
        after = reader.read(writer.write(doc));
        Testing.print(after);
        assertThat(after.getString("field1")).isEqualTo("value");
        assertThat(after.getInteger("field2")).isEqualTo(22);
        assertThat(after.size()).isEqualTo(3);
        Document nested = after.getDocument("field3");
        assertThat(nested.getString("a")).isEqualTo("A");
        assertThat(nested.getString("b")).isEqualTo("B");
        assertThat(nested.size()).isEqualTo(2);
    }

    @Test
    public void shouldWriteDocumentWithDeeplyNestedDocument() throws Exception {
        doc.set("field1", "value");
        doc.set("field2", 22);
        doc.set("field3", Document.create("a", "A", "b", "B", "c", Document.create("x", "X")));
        after = reader.read(writer.write(doc));
        Testing.print(after);
        assertThat(after.getString("field1")).isEqualTo("value");
        assertThat(after.getInteger("field2")).isEqualTo(22);
        assertThat(after.size()).isEqualTo(3);
        Document nested = after.getDocument("field3");
        assertThat(nested.getString("a")).isEqualTo("A");
        assertThat(nested.getString("b")).isEqualTo("B");
        assertThat(nested.size()).isEqualTo(3);
        Document deepNested = nested.getDocument("c");
        assertThat(deepNested.getString("x")).isEqualTo("X");
        assertThat(deepNested.size()).isEqualTo(1);
    }

}
