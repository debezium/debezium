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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Strings;

/**
 * An abstract regex implementation of {@link TopicNamingStrategy}.
 *
 * @author Harvey Yue
 */
@Incubating
public abstract class AbstractRegexTopicNamingStrategy extends AbstractTopicNamingStrategy<DataCollectionId> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRegexTopicNamingStrategy.class);

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
            .withValidation(AbstractRegexTopicNamingStrategy::validateTopicReplacement)
            .withDescription("The replacement string used in conjunction with " + TOPIC_REGEX.name() +
                    ". This will be used to create the new topic name.");

    public static final Field TOPIC_KEY_ENFORCE_UNIQUENESS = Field.create("topic.key.enforce.uniqueness")
            .withDisplayName("Add source topic name into key")
            .withType(ConfigDef.Type.BOOLEAN)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(true)
            .withDescription("Augment each record's key with a field denoting the source topic. This field " +
                    "distinguishes records coming from different physical tables which may otherwise have " +
                    "primary/unique key conflicts. If the source tables are guaranteed to have globally " +
                    "unique keys then this may be set to false to disable key rewriting.");

    public static final Field TOPIC_KEY_FIELD_NAME = Field.create("topic.key.field.name")
            .withDisplayName("Key field name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            // Default is prefixed with "__dbz__" to minimize the likelihood of a conflict with an existing key field name.
            .withDefault("__dbz__physicalTableIdentifier")
            .withDescription("Each record's key schema will be augmented with this field name. The purpose of this " +
                    "field is to distinguish the different physical tables that can now share a single topic. Make " +
                    "sure not to configure a field name that is at risk of conflict with existing key schema field " +
                    "names.");

    public static final Field TOPIC_KEY_FIELD_REGEX = Field.create("topic.key.field.regex")
            .withDisplayName("Key field regex")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isRegex)
            .withDescription("The regex used for extracting the physical table identifier from the original topic " +
                    "name. Now that multiple physical tables can share a topic, the event's key may need to be augmented " +
                    "to include fields other than just those for the record's primary/unique key, since these are not " +
                    "guaranteed to be unique across tables. We need some identifier added to the key that distinguishes " +
                    "the different physical tables.");

    public static final Field TOPIC_KEY_FIELD_REPLACEMENT = Field.create("topic.key.field.replacement")
            .withDisplayName("Key field replacement")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(AbstractRegexTopicNamingStrategy::validateKeyFieldReplacement)
            .withDescription("The replacement string used in conjunction with " + TOPIC_KEY_FIELD_REGEX.name() +
                    ". This will be used to create the physical table identifier in the record's key.");

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

    /**
     * If TOPIC_KEY_FIELD_REGEX has a value that is really a regex, then the TOPIC_KEY_FIELD_REPLACEMENT must be a non-empty value.
     */
    private static int validateKeyFieldReplacement(Configuration config, Field field, Field.ValidationOutput problems) {
        String keyFieldRegex = config.getString(TOPIC_KEY_FIELD_REGEX);
        if (keyFieldRegex != null) {
            keyFieldRegex = keyFieldRegex.trim();
        }

        String keyFieldReplacement = config.getString(TOPIC_KEY_FIELD_REPLACEMENT);
        if (keyFieldReplacement != null) {
            keyFieldReplacement = keyFieldReplacement.trim();
        }

        if (!Strings.isNullOrEmpty(keyFieldRegex) && Strings.isNullOrEmpty(keyFieldReplacement)) {
            problems.accept(
                    TOPIC_KEY_FIELD_REPLACEMENT,
                    null,
                    String.format("%s must be non-empty if %s is set.",
                            TOPIC_KEY_FIELD_REPLACEMENT.name(),
                            TOPIC_KEY_FIELD_REGEX.name()));

            return 1;
        }

        return 0;
    }

    private Pattern topicRegex;
    private String topicReplacement;
    private boolean keyEnforceUniqueness;
    private String keyFieldName;
    private Pattern keyFieldRegex;
    private String keyFieldReplacement;
    private BoundedConcurrentHashMap<String, String> keyRegexReplaceCache;

    public AbstractRegexTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public void configure(Properties props) {
        super.configure(props);
        Configuration config = Configuration.from(props);
        final Field.Set regexConfigFields = Field.setOf(
                TOPIC_REGEX,
                TOPIC_REPLACEMENT,
                TOPIC_KEY_ENFORCE_UNIQUENESS,
                TOPIC_KEY_FIELD_NAME,
                TOPIC_KEY_FIELD_REGEX,
                TOPIC_KEY_FIELD_REPLACEMENT);

        if (!config.validateAndRecord(regexConfigFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        topicRegex = Pattern.compile(config.getString(TOPIC_REGEX));
        topicReplacement = config.getString(TOPIC_REPLACEMENT);

        keyEnforceUniqueness = config.getBoolean(TOPIC_KEY_ENFORCE_UNIQUENESS);
        keyFieldName = config.getString(TOPIC_KEY_FIELD_NAME);
        String keyFieldRegexString = config.getString(TOPIC_KEY_FIELD_REGEX);
        if (keyFieldRegexString != null) {
            keyFieldRegexString = keyFieldRegexString.trim();
        }
        if (!Strings.isNullOrBlank(keyFieldRegexString)) {
            keyFieldRegex = Pattern.compile(config.getString(TOPIC_KEY_FIELD_REGEX));
            keyFieldReplacement = config.getString(TOPIC_KEY_FIELD_REPLACEMENT);
        }

        keyRegexReplaceCache = new BoundedConcurrentHashMap<>(
                config.getInteger(TOPIC_CACHE_SIZE),
                10,
                BoundedConcurrentHashMap.Eviction.LRU);
    }

    @Override
    public String dataChangeTopic(DataCollectionId id) {
        String oldTopic = getOriginTopic(id);
        return determineNewTopic(id, sanitizedTopicName(oldTopic));
    }

    public abstract String getOriginTopic(DataCollectionId id);

    /**
     * Determine the new topic name.
     *
     * @param tableId  the table id
     * @param oldTopic the name of the old topic
     * @return return the new topic name, if the regex applies. Otherwise, return original topic.
     */
    protected String determineNewTopic(DataCollectionId tableId, String oldTopic) {
        String newTopic = topicNames.get(tableId);
        if (newTopic == null) {
            newTopic = oldTopic;
            final Matcher matcher = topicRegex.matcher(oldTopic);
            if (matcher.matches()) {
                newTopic = matcher.replaceFirst(topicReplacement);
                if (newTopic.isEmpty()) {
                    LOGGER.warn("Routing regex returned an empty topic name, propagating original topic");
                    newTopic = oldTopic;
                }
            }
            topicNames.put(tableId, newTopic);
        }
        return newTopic;
    }

    @Override
    public TopicSchemaAugment<SchemaBuilder> keySchemaAugment() {
        return schemaBuilder -> {
            if (keyEnforceUniqueness) {
                schemaBuilder.field(keyFieldName, Schema.STRING_SCHEMA);
                return true;
            }
            return false;
        };
    }

    @Override
    public TopicValueAugment<DataCollectionId, Schema, Struct> keyValueAugment() {
        return (id, schema, struct) -> {
            if (keyEnforceUniqueness) {
                String oldTopic = getOriginTopic(id);
                String physicalTableIdentifier = oldTopic;
                if (keyFieldRegex != null) {
                    physicalTableIdentifier = keyRegexReplaceCache.get(oldTopic);
                    if (physicalTableIdentifier == null) {
                        final Matcher matcher = keyFieldRegex.matcher(oldTopic);
                        if (matcher.matches()) {
                            physicalTableIdentifier = matcher.replaceFirst(keyFieldReplacement);
                        }
                        else {
                            physicalTableIdentifier = oldTopic;
                        }
                        keyRegexReplaceCache.put(oldTopic, physicalTableIdentifier);
                    }
                }
                struct.put(schema.field(keyFieldName), physicalTableIdentifier);
                return true;
            }
            return false;
        };
    }
}
