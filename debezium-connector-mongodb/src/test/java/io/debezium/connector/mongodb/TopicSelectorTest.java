/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.fest.assertions.StringAssert;
import org.junit.Before;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class TopicSelectorTest {

    private TopicSelector noPrefix;
    private TopicSelector withPrefix;
    
    @Before
    public void beforeEach() {
        noPrefix = TopicSelector.defaultSelector(null);
        withPrefix = TopicSelector.defaultSelector("prefix");
    }
    
    @Test
    public void shouldHandleCollectionIdWithDatabaseAndCollection() {
        assertTopic(noPrefix,dbAndCollection("db","coll")).isEqualTo("db.coll");
        assertTopic(withPrefix,dbAndCollection("db","coll")).isEqualTo("prefix.db.coll");
    }
    
    protected StringAssert assertTopic(TopicSelector selector, CollectionId collectionId) {
        return assertThat(selector.getTopic(collectionId));
    }
    
    protected CollectionId dbAndCollection( String dbName, String collectionName ) {
        return new CollectionId("rs0",dbName,collectionName);
    }

}
