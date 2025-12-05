/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class CollectionIdTest {

    private CollectionId id;

    @Test
    public void shouldParseString() {
        assertParseable("b", "c");
    }

    @Test
    public void shouldParseStringWithDottedCollection() {
        assertParseable("b", "c.d");
    }

    @Test
    public void shouldNotParseStringWithDotAtStart() {
        assertThat(CollectionId.parse(".a.b")).isNull();
    }

    @Test
    public void shouldNotParseStringWithDotAtEnd() {
        assertThat(CollectionId.parse("a.")).isNull();
    }

    @Test
    public void shouldNotParseStringWithOneSegment() {
        assertThat(CollectionId.parse("a")).isNull();
    }

    @Test
    public void shouldNotFullParseStringWithDot() {
        final CollectionId collectionId = CollectionId.parse("a.b.c");
        assertThat(collectionId.dbName()).isEqualTo("a");
        assertThat(collectionId.name()).isEqualTo("b.c");
    }

    @Test
    public void shouldNotFullParseStringWithDotAtStart() {
        assertThat(CollectionId.parse(".rs0.a.b")).isNull();
    }

    @Test
    public void shouldNotParseFullStringWithDotAtEnd() {
        assertThat(CollectionId.parse("a.")).isNull();
    }

    @Test
    public void shouldNotParseFullStringWithMissingSegment() {
        assertThat(CollectionId.parse("a")).isNull();
    }

    protected void assertParseable(String dbName, String collectionName) {
        String str = dbName + "." + collectionName;
        id = CollectionId.parse(str);
        assertThat(id.dbName()).isEqualTo(dbName);
        assertThat(id.name()).isEqualTo(collectionName);
    }

}
