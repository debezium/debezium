/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import org.fest.assertions.StringAssert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.schema.TopicSelector;

/**
 * @author Randall Hauch
 *
 */
public class TopicSelectorTest {

    private TopicSelector<CollectionId> noPrefix;
    private TopicSelector<CollectionId> withPrefix;

    @Before
    public void beforeEach() {
        noPrefix = MongoDbTopicSelector.defaultSelector(null, "__debezium-heartbeat");
        withPrefix = MongoDbTopicSelector.defaultSelector("prefix", "__debezium-heartbeat");
    }

    @Test
    public void shouldHandleCollectionIdWithDatabaseAndCollection() {
        assertTopic(noPrefix,dbAndCollection("db", "coll")).isEqualTo("db.coll");
        assertTopic(withPrefix,dbAndCollection("db", "coll")).isEqualTo("prefix.db.coll");
    }

    protected StringAssert assertTopic(TopicSelector<CollectionId> selector, CollectionId collectionId) {
        return assertThat(selector.topicNameFor(collectionId));
    }

    protected CollectionId dbAndCollection( String dbName, String collectionName ) {
        return new CollectionId("rs0",dbName,collectionName);
    }

}
