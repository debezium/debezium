/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import com.datapipeline.clients.error.DpError;
import com.datapipeline.clients.mongodb.MongodbSchemaConfig;
import com.datapipeline.clients.mongodb.MongodbSchemaNameConfig;
import com.dp.internal.bean.DataSourceSchemaMappingExemption;
import com.dp.internal.bean.DpErrorCode;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.util.Clock;

/**
 * @author Randall Hauch
 */
public class ReplicationContext extends ConnectionContext {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Filters filters;
    private final SourceInfo source;
    private final Clock clock = Clock.system();
    private final TopicSelector topicSelector;

    /**
     * @param config the configuration
     */
    public ReplicationContext(Configuration config) {
        super(config);

        final String serverName = config.getString(MongoDbConnectorConfig.LOGICAL_NAME);
        this.filters = new Filters(config);
        try {
            MongoDBSchemaCache schemaCache = new MongoDBSchemaCache(objectMapper.readValue
                (config.getString(MongoDbConnectorConfig.COLLECTION_SCHEMA_CONFIGURATION),
                    new TypeReference<List<MongodbSchemaConfig>>() {
                    }));
            List<DataSourceSchemaMappingExemption> exemptions = objectMapper.readValue(config.getString(MongoDbConnectorConfig.SCHEMA_MAPPING_EXEMPTION),
                new TypeReference<List<DataSourceSchemaMappingExemption>>() {
                });
            List<MongodbSchemaNameConfig> schemaNameConfigs =  objectMapper.readValue(config.getString(MongoDbConnectorConfig.COLLECTION_SCHEMA_NAME_CONFIGURATION),
                new TypeReference<List<MongodbSchemaNameConfig>>() {});
            this.source = new SourceInfo(config.getString(MongoDbConnectorConfig.DP_TASK_ID), serverName, schemaCache, exemptions);
            this.topicSelector = new DpTopicSelector(schemaNameConfigs, serverName);
        } catch (IOException e) {
            logger.error("Error deserialize collection schema config.", e);
            new DpError(e, e.getMessage(), String.valueOf(config.getInteger(MongoDbConnectorConfig.DP_TASK_ID)), null, DpErrorCode.CRITICAL_ERROR);
            throw new ConnectException("Error deserialize mongodb config.", e);
        }

    }

    @Override
    protected Logger logger() {
        return logger;
    }

    public TopicSelector topicSelector() {
        return topicSelector;
    }

    public Predicate<CollectionId> collectionFilter() {
        return filters.collectionFilter();
    }

    public SourceInfo source() {
        return source;
    }

    public Clock clock() {
        return clock;
    }
}
