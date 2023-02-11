/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.StringJoiner;

import io.debezium.annotation.ThreadSafe;
import io.debezium.schema.TopicSelector;

/**
 * Factory for this connector's {@link TopicSelector}.
 *
 * @author Randall Hauch
 * @deprecated Use {@link io.debezium.schema.DefaultTopicNamingStrategy} instead.
 */
@Deprecated
@ThreadSafe
public class MongoDbTopicSelector {

    /**
     * Gets the selector for topics for a given prefix, database name, and collection name. This selector returns
     * "{@code <prefix>.<dbName>.<collectionName>}", and does not use the replica set name.
     *
     * @param prefix the prefix; be null or empty if no prefix is needed
     * @param heartbeatPrefix the name of the prefix to be used for all heartbeat topics; may not be null and must not terminate in the
     *            {@code delimiter}
     * @return the topic selector; never null
     */
    public static TopicSelector<CollectionId> defaultSelector(String prefix, String heartbeatPrefix) {
        return TopicSelector.defaultSelector(prefix, heartbeatPrefix, ".",
                (id, pref, delimiter) -> getTopicName(id, pref, delimiter));
    }

    /**
     * Get the name of the topic for a given prefix, database name, and collection name. This method returns
     * "{@code <prefix><delimiter><dbName><delimiter><collectionName>}", and does not use the replica set name.
     *
     * @param delimiter the string delineating the prefix, database, and collection names; may not be null
     * @param prefix the prefix; be null or empty if no prefix is needed
     * @return the topic selector; never null
     */
    private static String getTopicName(CollectionId collectionId, String prefix, String delimiter) {
        StringJoiner sb = new StringJoiner(delimiter);

        if (prefix != null && prefix.trim().length() > 0) {
            String trimmedPrefix = prefix.trim();
            sb.add(trimmedPrefix);
        }

        sb.add(collectionId.dbName());
        sb.add(collectionId.name());

        return sb.toString();
    }
}
