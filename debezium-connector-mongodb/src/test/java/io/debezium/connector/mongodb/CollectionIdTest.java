/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Randall Hauch
 *
 */
public class CollectionIdTest {

    private CollectionId id;

    @Test
    void shouldParseString() {
        assertParseable("b", "c");
    }

    @Test
    void shouldParseStringWithDottedCollection() {
        assertParseable("b", "c.d");
    }

    @Test
    void shouldNotParseStringWithDotAtStart() {
        assertThat(CollectionId.parse(".a.b")).isNull();
    }

    @Test
    void shouldNotParseStringWithDotAtEnd() {
        assertThat(CollectionId.parse("a.")).isNull();
    }

    @Test
    void shouldNotParseStringWithOneSegment() {
        assertThat(CollectionId.parse("a")).isNull();
    }

    @Test
    void shouldNotFullParseStringWithDot() {
        final CollectionId collectionId = CollectionId.parse("a.b.c");
        assertThat(collectionId.dbName()).isEqualTo("a");
        assertThat(collectionId.name()).isEqualTo("b.c");
    }

    @Test
    void shouldNotFullParseStringWithDotAtStart() {
        assertThat(CollectionId.parse(".rs0.a.b")).isNull();
    }

    @Test
    void shouldNotParseFullStringWithDotAtEnd() {
        assertThat(CollectionId.parse("a.")).isNull();
    }

    @Test
    void shouldNotParseFullStringWithMissingSegment() {
        assertThat(CollectionId.parse("a")).isNull();
    }

    protected void assertParseable(String dbName, String collectionName) {
        String str = dbName + "." + collectionName;
        id = CollectionId.parse(str);
        assertThat(id.dbName()).isEqualTo(dbName);
        assertThat(id.name()).isEqualTo(collectionName);
    }

}
