/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class CollectionIdTest {

    private CollectionId id;

    @Test
    public void shouldParseString() {
        assertParseable("a", "b", "c");
    }

    @Test
    public void shouldParseStringWithDottedCollection() {
        assertParseable("a", "b", "c.d");
    }

    @Test
    public void shouldNotParseStringWithDotAtStart() {
        assertThat(CollectionId.parse("rs0", ".a.b")).isNull();
    }

    @Test
    public void shouldNotParseStringWithDotAtEnd() {
        assertThat(CollectionId.parse("rs0", "a.")).isNull();
    }

    @Test
    public void shouldNotParseStringWithOneSegment() {
        assertThat(CollectionId.parse("rs0", "a")).isNull();
    }

    protected void assertParseable(String replicaSetName, String dbName, String collectionName) {
        String str = dbName + "." + collectionName;
        id = CollectionId.parse(replicaSetName, str);
        assertThat(id.replicaSetName()).isEqualTo(replicaSetName);
        assertThat(id.dbName()).isEqualTo(dbName);
        assertThat(id.name()).isEqualTo(collectionName);
    }

}
