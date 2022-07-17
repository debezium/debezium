/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * Implement a regex expression strategy to determine data event topic names using {@link DataCollectionId#databaseParts()}.
 *
 * @author Harvey Yue
 */
@Incubating
public class DefaultRegexTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultRegexTopicNamingStrategy.class);

    public static final Field TOPIC_REGEX = Field.create("topic.regex")
            .withDisplayName("Topic regex")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .required()
            .withValidation(Field::isRegex)
            .withDescription("The regex used for extracting the name of the logical table from the original topic name.");

    public static final Field TOPIC_REPLACEMENT = Field.create("topic.replacement")
            .withDisplayName("Topic replacement")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .required()
            .withValidation(DefaultRegexTopicNamingStrategy::validateTopicReplacement)
            .withDescription("The replacement string used in conjunction with " + TOPIC_REGEX.name() +
                    ". This will be used to create the new topic name.");

    /**
     * If TOPIC_REGEX has a value that is really a regex, then the TOPIC_REPLACEMENT must be a non-empty value.
     */
    private static int validateTopicReplacement(Configuration config, Field field, Field.ValidationOutput problems) {
        String topicRegex = config.getString(TOPIC_REGEX);
        if (topicRegex != null) {
            topicRegex = topicRegex.trim();
        }

        String topicReplacement = config.getString(TOPIC_REPLACEMENT);
        if (topicReplacement != null) {
            topicReplacement = topicReplacement.trim();
        }

        if (!Strings.isNullOrEmpty(topicRegex) && Strings.isNullOrEmpty(topicReplacement)) {
            problems.accept(
                    TOPIC_REPLACEMENT,
                    null,
                    String.format("%s must be non-empty if %s is set.",
                            TOPIC_REPLACEMENT.name(),
                            TOPIC_REGEX.name()));

            return 1;
        }

        return 0;
    }

    private Pattern topicRegex;
    private String topicReplacement;

    public DefaultRegexTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public void configure(Properties props) {
        super.configure(props);
        Configuration config = Configuration.from(props);
        topicRegex = Pattern.compile(config.getString(TOPIC_REGEX));
        topicReplacement = config.getString(TOPIC_REPLACEMENT);
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        String oldTopic = mkString(Collect.arrayListOf(prefix, id.databaseParts()), delimiter);
        return determineNewTopic(id, sanitizedTopicName(oldTopic));
    }

    /**
     * Determine the new topic name.
     *
     * @param tableId  the table id
     * @param oldTopic the name of the old topic
     * @return return the new topic name, if the regex applies. Otherwise, return original topic.
     */
    private String determineNewTopic(DataCollectionId tableId, String oldTopic) {
        String newTopic = topicNames.get(tableId);
        if (newTopic == null) {
            newTopic = oldTopic;
            final Matcher matcher = topicRegex.matcher(oldTopic);
            if (matcher.matches()) {
                newTopic = matcher.replaceFirst(topicReplacement);
                if (newTopic.isEmpty()) {
                    LOGGER.warn("Routing regex returned an empty topic name, propagating original topic");
                }
            }
            topicNames.put(tableId, newTopic);
        }
        return newTopic;
    }
}
