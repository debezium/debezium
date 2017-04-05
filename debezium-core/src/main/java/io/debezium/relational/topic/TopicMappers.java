/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Table;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author David Leibovic
 */
public class TopicMappers {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private List<TopicMapper> topicMappers;

    public TopicMappers(Configuration config, String[] topicMapperAliases) {
        topicMappers = new ArrayList<>();
        if (topicMapperAliases != null) {
            for (String alias : topicMapperAliases) {
                TopicMapper topicMapper = config.getInstance("topic.mappers." + alias + ".type", TopicMapper.class);
                if (topicMapper != null) {
                    topicMappers.add(topicMapper);
                    validateTopicMapperConfig(topicMapper, alias, config);
                }
            }
        }
        TopicMapper defaultTopicMapper = new ByTableTopicMapper();
        topicMappers.add(defaultTopicMapper);
        validateTopicMapperConfig(defaultTopicMapper, null, config);
    }

    /**
     * Get the topic mapper to use for the given table.
     *
     * @param topicPrefix prefix for the topic
     * @param table the table that we are getting the topic name for
     * @return the TopicMapper; never null
     */
    public TopicMapper getTopicMapperToUse(String topicPrefix, Table table) {
        for (TopicMapper topicMapper : topicMappers) {
            if (topicMapper.getTopicName(topicPrefix, table) != null) {
                return topicMapper;
            }
        }
        return topicMappers.get(topicMappers.size() - 1);
    }

    private void validateTopicMapperConfig(TopicMapper topicMapper, String alias, Configuration config) {
        if (alias == null) {
            return;
        }
        Configuration topicMapperConfig = config.subset("topic.mappers." + alias + ".", true);
        topicMapper.setConfig(topicMapperConfig);
        Field.Set topicMapperConfigFields = topicMapper.configFields();
        if (topicMapperConfigFields != null && !topicMapperConfig.validateAndRecord(topicMapperConfigFields, logger::error)) {
            throw new ConnectException("Unable to validate config for topic mapper: " + topicMapper.getClass() +
                    " with alias " + alias + ".");
        }
    }

}
