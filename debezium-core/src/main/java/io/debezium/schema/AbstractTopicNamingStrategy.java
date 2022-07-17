/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.BoundedConcurrentHashMap;
import io.debezium.util.Strings;

/**
 * An abstract implementation of {@link TopicNamingStrategy}.
 *
 * @author Harvey Yue
 */
@Incubating
public abstract class AbstractTopicNamingStrategy<I extends DataCollectionId> implements TopicNamingStrategy<I> {
    protected static final String LOGIC_NAME_PLACEHOLDER = "${logical.name}";
    protected static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_.\\-]+$");
    public static final String DEFAULT_HEARTBEAT_TOPIC_PREFIX = "__debezium-heartbeat";
    public static final String DEFAULT_TRANSACTION_TOPIC = "transaction";

    public static final Field TOPIC_DELIMITER = Field.create("topic.delimiter")
            .withDisplayName("Topic delimiter")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(".")
            .withValidation(AbstractTopicNamingStrategy::validateTopicName)
            .withDescription("Specify the delimiter for topic name.");

    public static final Field TOPIC_PREFIX = Field.create("topic.prefix")
            .withDisplayName("Topic prefix")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(LOGIC_NAME_PLACEHOLDER)
            .withValidation(AbstractTopicNamingStrategy::validateTopicName)
            .withDescription("The name of the prefix to be used for all topics, the placeholder " + LOGIC_NAME_PLACEHOLDER +
                    " can be used for referring to the connector's logical name as default value.");

    public static final Field TOPIC_CACHE_SIZE = Field.create("topic.cache.size")
            .withDisplayName("Topic cache size")
            .withType(ConfigDef.Type.INT)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(10000)
            .withDescription("The size used for holding the topic names in bounded concurrent hash map. The cache " +
                    "will help to determine the topic name corresponding to a given data collection");

    public static final Field TOPIC_HEARTBEAT_PREFIX = Field.create("topic.heartbeat.prefix")
            .withDisplayName("Prefix name of heartbeat topic")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DEFAULT_HEARTBEAT_TOPIC_PREFIX)
            .withValidation(AbstractTopicNamingStrategy::validateTopicName)
            .withDescription("Specify the heartbeat topic name. Defaults to " +
                    DEFAULT_HEARTBEAT_TOPIC_PREFIX + "." + LOGIC_NAME_PLACEHOLDER);

    public static final Field TOPIC_TRANSACTION = Field.create("topic.transaction")
            .withDisplayName("Transaction topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DEFAULT_TRANSACTION_TOPIC)
            .withValidation(AbstractTopicNamingStrategy::validateTopicName)
            .withDescription("Specify the transaction topic name. Defaults to " +
                    LOGIC_NAME_PLACEHOLDER + "." + DEFAULT_TRANSACTION_TOPIC);

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTopicNamingStrategy.class);

    protected BoundedConcurrentHashMap<I, String> topicNames;
    protected String logicalName;
    protected String delimiter;
    protected String prefix;
    protected String transaction;
    protected String heartbeatPrefix;

    public AbstractTopicNamingStrategy(Properties props) {
        this.logicalName = props.getProperty(RelationalDatabaseConnectorConfig.LOGICAL_NAME);
        assert logicalName != null;
        this.configure(props);
    }

    public AbstractTopicNamingStrategy(Properties props, String logicalName) {
        this.logicalName = logicalName;
        this.configure(props);
    }

    @Override
    public void configure(Properties props) {
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(
                TOPIC_PREFIX,
                TOPIC_DELIMITER,
                TOPIC_CACHE_SIZE,
                TOPIC_TRANSACTION,
                TOPIC_HEARTBEAT_PREFIX);

        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        topicNames = new BoundedConcurrentHashMap<>(
                config.getInteger(TOPIC_CACHE_SIZE),
                10,
                BoundedConcurrentHashMap.Eviction.LRU);
        delimiter = config.getString(TOPIC_DELIMITER);
        if (config.getString(TOPIC_PREFIX).equals(LOGIC_NAME_PLACEHOLDER)) {
            prefix = logicalName;
        }
        else {
            prefix = config.getString(TOPIC_PREFIX);
        }
        heartbeatPrefix = config.getString(TOPIC_HEARTBEAT_PREFIX);
        transaction = config.getString(TOPIC_TRANSACTION);
    }

    @Override
    public abstract String dataChangeTopic(I id);

    @Override
    public String schemaChangeTopic() {
        return prefix;
    }

    @Override
    public String heartbeatTopic() {
        return String.join(delimiter, heartbeatPrefix, prefix);
    }

    @Override
    public String transactionTopic() {
        return String.join(delimiter, prefix, transaction);
    }

    protected String mkString(List<String> data, String delimiter) {
        return data.stream().filter(f -> !Strings.isNullOrBlank(f)).collect(Collectors.joining(delimiter));
    }

    protected static int validateTopicName(Configuration config, Field field, Field.ValidationOutput problems) {
        String name = config.getString(field);

        if (name.equals(LOGIC_NAME_PLACEHOLDER)) {
            return 0;
        }

        if (name != null) {
            if (!TOPIC_NAME_PATTERN.asPredicate().test(name)) {
                problems.accept(field, name, name + " has invalid format (only the underscore, hyphen, dot and alphanumeric characters are allowed)");
                return 1;
            }
        }
        return 0;
    }
}
