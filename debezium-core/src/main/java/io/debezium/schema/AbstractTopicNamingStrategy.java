/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
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
    public static final String DEFAULT_HEARTBEAT_TOPIC_PREFIX = "__debezium-heartbeat";
    public static final String DEFAULT_TRANSACTION_TOPIC = "transaction";

    public static final Field TOPIC_DELIMITER = Field.create("topic.delimiter")
            .withDisplayName("Topic delimiter")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(".")
            .withValidation(CommonConnectorConfig::validateTopicName)
            .withDescription("Specify the delimiter for topic name.");

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
            .withValidation(CommonConnectorConfig::validateTopicName)
            .withDescription("Specify the heartbeat topic name. Defaults to " +
                    DEFAULT_HEARTBEAT_TOPIC_PREFIX + ".${topic.prefix}");

    public static final Field TOPIC_TRANSACTION = Field.create("topic.transaction")
            .withDisplayName("Transaction topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DEFAULT_TRANSACTION_TOPIC)
            .withValidation(CommonConnectorConfig::validateTopicName)
            .withDescription("Specify the transaction topic name. Defaults to " +
                    "${topic.prefix}." + DEFAULT_TRANSACTION_TOPIC);

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTopicNamingStrategy.class);

    protected BoundedConcurrentHashMap<I, String> topicNames;
    protected String delimiter;
    protected String prefix;
    protected String transaction;
    protected String heartbeatPrefix;

    public AbstractTopicNamingStrategy(Properties props) {
        this.configure(props);
    }

    @Override
    public void configure(Properties props) {
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(
                CommonConnectorConfig.TOPIC_PREFIX,
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
        heartbeatPrefix = config.getString(TOPIC_HEARTBEAT_PREFIX);
        transaction = config.getString(TOPIC_TRANSACTION);
        prefix = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
        assert prefix != null;
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
}
