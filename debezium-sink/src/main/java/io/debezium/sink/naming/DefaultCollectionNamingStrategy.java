/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.sink.naming;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.sink.DebeziumSinkRecord;

/**
 * Default implementation of the {@link CollectionNamingStrategy} where the table name is driven
 * directly from the topic name, replacing any {@code dot} characters with {@code underscore}
 * and source field in topic.
 *
 * @author Chris Cranford
 * @author rk3rn3r
 */
public class DefaultCollectionNamingStrategy implements CollectionNamingStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCollectionNamingStrategy.class);

    private static final String ENVELOPE_SOURCE_FIELD_NAME = "source";

    private final Pattern sourcePattern = Pattern.compile("\\$\\{(source\\.)(.*?)}");

    @Override
    public String resolveCollectionName(DebeziumSinkRecord record, String collectionNameFormat) {
        // Default behavior is to replace dots with underscores
        final String topicName = record.topicName().replace(".", "_");
        String collection = collectionNameFormat.replace("${topic}", topicName);

        collection = resolveCollectionNameBySource(record, collection, collectionNameFormat);
        return collection;
    }

    private String resolveCollectionNameBySource(DebeziumSinkRecord record, String collectionName, String collectionNameFormat) {
        if (collectionName.contains("${source.")) {
            if (!record.isDebeziumMessage()) {
                LOGGER.warn(
                        "Ignore this record because it isn't a Debezium record, then cannot resolve a collection name in topic '{}', partition '{}', offset '{}'",
                        record.topicName(), record.partition(), record.offset());
                return null;
            }

            try {
                Struct source = ((Struct) record.value()).getStruct(ENVELOPE_SOURCE_FIELD_NAME);
                Matcher matcher = sourcePattern.matcher(collectionName);
                while (matcher.find()) {
                    String target = matcher.group();
                    collectionName = collectionName.replace(target, source.getString(matcher.group(2)));
                }
            }
            catch (DataException e) {
                LOGGER.error("Failed to resolve collection name with format '{}', check source field in topic '{}'",
                        collectionNameFormat, record.topicName(), e);
                throw e;
            }
        }
        return collectionName;
    }

}
