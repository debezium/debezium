/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class CollectionIdTest {

    private CollectionId id;
    
    @Test
    public void shouldParseStringWithThreeSegments() {
        assertParseable("a","b","c");
    }
    
    @Test
    public void shouldNotParseStringWithTwoSegments() {
        assertThat(CollectionId.parse("a.b")).isNull();
    }
    
    @Test
    public void shouldNotParseStringWithOneSegments() {
        assertThat(CollectionId.parse("a")).isNull();
    }
    
    protected void assertParseable( String replicaSetName, String dbName, String collectionName ) {
        String str = replicaSetName + "." + dbName + "." + collectionName;
        id = CollectionId.parse(str);
        assertThat(id.replicaSetName()).isEqualTo(replicaSetName);
        assertThat(id.dbName()).isEqualTo(dbName);
        assertThat(id.name()).isEqualTo(collectionName);
    }

}
