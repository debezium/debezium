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

import io.debezium.doc.FixFor;
import io.debezium.schema.TopicSelector;

/**
 * @author Randall Hauch
 *
 */
public class TopicSelectorTest {

    private TopicSelector<CollectionId> defaultNoPrefix;
    private TopicSelector<CollectionId> defaultWithPrefix;

    private TopicSelector<CollectionId> noPrefix;
    private TopicSelector<CollectionId> withPrefix;

    @Before
    public void beforeEach() {
        // default topic delimiter
        defaultNoPrefix = MongoDbTopicSelector.defaultSelector(null, "__debezium-heartbeat");
        defaultWithPrefix = MongoDbTopicSelector.defaultSelector("prefix", "__debezium-heartbeat");

        // topic delimiter = _
        noPrefix = MongoDbTopicSelector.selector(null, "__debezium-heartbeat", "_");
        withPrefix = MongoDbTopicSelector.selector("prefix", "__debezium-heartbeat", "_");
    }

    @Test
    public void shouldHandleCollectionIdWithDatabaseAndCollection() {
        // default topic delimiter
        assertTopic(defaultNoPrefix, dbAndCollection("db", "coll")).isEqualTo("db.coll");
        assertTopic(defaultWithPrefix, dbAndCollection("db", "coll")).isEqualTo("prefix.db.coll");

        // topic delimiter = _
        assertTopic(noPrefix, dbAndCollection("db", "coll")).isEqualTo("db_coll");
        assertTopic(withPrefix, dbAndCollection("db", "coll")).isEqualTo("prefix_db_coll");
    }

    @Test
    @FixFor("DBZ-878")
    public void shouldHandleCollectionIdWithInvalidTopicNameChar() {
        assertTopic(defaultNoPrefix, dbAndCollection("db", "my@collection")).isEqualTo("db.my_collection");
        assertTopic(defaultWithPrefix, dbAndCollection("db", "my@collection")).isEqualTo("prefix.db.my_collection");
    }

    protected StringAssert assertTopic(TopicSelector<CollectionId> selector, CollectionId collectionId) {
        return assertThat(selector.topicNameFor(collectionId));
    }

    protected CollectionId dbAndCollection(String dbName, String collectionName) {
        return new CollectionId("rs0", dbName, collectionName);
    }

}
