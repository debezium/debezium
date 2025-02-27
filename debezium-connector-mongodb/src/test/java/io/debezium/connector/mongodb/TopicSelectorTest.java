/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractStringAssert;
import org.junit.Before;
import org.junit.Test;

import io.debezium.doc.FixFor;
import io.debezium.schema.TopicSelector;

/**
 * @author Randall Hauch
 *
 */
@Deprecated
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
        assertTopic(noPrefix, dbAndCollection("db", "coll")).isEqualTo("db.coll");
        assertTopic(withPrefix, dbAndCollection("db", "coll")).isEqualTo("prefix.db.coll");
    }

    @Test
    @FixFor("DBZ-878")
    public void shouldHandleCollectionIdWithInvalidTopicNameChar() {
        assertTopic(noPrefix, dbAndCollection("db", "my@collection")).isEqualTo("db.my_collection");
        assertTopic(withPrefix, dbAndCollection("db", "my@collection")).isEqualTo("prefix.db.my_collection");
    }

    protected AbstractStringAssert<?> assertTopic(TopicSelector<CollectionId> selector, CollectionId collectionId) {
        return assertThat(selector.topicNameFor(collectionId));
    }

    protected CollectionId dbAndCollection(String dbName, String collectionName) {
        return new CollectionId(dbName, collectionName);
    }

}
