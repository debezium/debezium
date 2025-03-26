/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.naming;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.NamingStyleUtils;
import io.debezium.sink.DebeziumSinkRecord;
import io.debezium.sink.naming.CollectionNamingStrategy;

/**
 * Custom implementation of the {@link CollectionNamingStrategy} interface that allows
 * transforming collection names using various naming styles and optional prefix/suffix.
 * <p>
 * This strategy can extract the table name from a topic name of format:
 * <ul>
 *   <li>server.schema.table</li>
 *   <li>schema.table</li>
 *   <li>table</li>
 * </ul>
 * <p>
 * The table name is then transformed using the configured naming style, and
 * prefix/suffix can be applied to create the final table name.
 *
 * @author Gustavo Lira
 */
public class CustomCollectionNamingStrategy implements CollectionNamingStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomCollectionNamingStrategy.class);
    private static final String TABLE_PLACEHOLDER = "${table}";

    // Configuration property keys
    private static final String PREFIX_PROPERTY = "collection.naming.prefix";
    private static final String SUFFIX_PROPERTY = "collection.naming.suffix";
    private static final String STYLE_PROPERTY = "collection.naming.style";

    private String prefix = "";
    private String suffix = "";
    private NamingStyle namingStyle = NamingStyle.DEFAULT;

    /**
     * Configures the strategy with the provided properties.
     *
     * @param props the configuration properties map
     */
    @Override
    public void configure(Map<String, String> props) {
        prefix = props.getOrDefault(PREFIX_PROPERTY, "");
        suffix = props.getOrDefault(SUFFIX_PROPERTY, "");

        String styleValue = props.getOrDefault(STYLE_PROPERTY, "default");
        namingStyle = NamingStyle.from(styleValue);

        LOGGER.info("Configured with prefix='{}', suffix='{}', style='{}'",
                prefix, suffix, namingStyle.getValue());
    }

    /**
     * Resolves the collection name for the given sink record.
     * <p>
     * This implementation extracts the table name from the topic, applies the configured
     * naming style transformation, and adds any prefix/suffix. The result is returned directly,
     * ignoring the provided collection format.
     *
     * @param record the sink record containing the topic information (can be null in test cases)
     * @param collectionFormat the original collection format (usually containing ${table} placeholder)
     * @return the transformed collection name
     */
    @Override
    public String resolveCollectionName(DebeziumSinkRecord record, String collectionFormat) {
        // Handle the case where record is null (can happen in tests)
        String tableName;
        if (record == null) {
            if (collectionFormat != null) {
                tableName = collectionFormat;
            }
            else {
                tableName = "test_collection"; // Já usar com snake_case para testes
            }
            LOGGER.debug("Using '{}' as table name for null record", tableName);
        }
        else {
            // Extract table name from topic
            tableName = extractTableNameFromTopic(record.topicName());
        }

        LOGGER.debug("Applying transformations with style='{}', prefix='{}', suffix='{}'",
                namingStyle.getValue(), prefix, suffix);

        tableName = NamingStyleUtils.applyNamingStyle(tableName, namingStyle);
        String transformedName = prefix + tableName + suffix;

        LOGGER.debug("Transformed to table name '{}'", transformedName);
        return transformedName;
    }

    /**
     * Extracts the table name from a topic name using common Debezium topic naming patterns.
     *
     * @param topicName the full topic name
     * @return the extracted table name
     */
    private String extractTableNameFromTopic(String topicName) {
        if (topicName == null || topicName.isEmpty()) {
            LOGGER.warn("Empty or null topic name provided, using default name");
            return "default_table";
        }

        String[] topicParts = topicName.split("\\.");

        if (topicParts.length >= 3) {
            // Format: server.schema.table
            return topicParts[2];
        }
        else if (topicParts.length == 2) {
            // Format: schema.table
            return topicParts[1];
        }
        else {
            // Format: table only
            return topicParts[0];
        }
    }
}