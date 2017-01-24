/*
 * Copyright Datapipeline Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import com.datapipeline.base.mongodb.MongodbSchemaNameConfig;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

public class DpTopicSelector implements TopicSelector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String DELIMITER = ".";

    private String prefix;

    private final Map<String, String> collectionSchemaNameMap = new ConcurrentHashMap<>();

    public DpTopicSelector(Configuration config, ObjectMapper objectMapper, String prefix){
        if(prefix != null && prefix.trim().length() > 0){
            this.prefix = prefix.trim();
        }
        try{
            List<MongodbSchemaNameConfig> schemaNameConfigs =  objectMapper.readValue(config.getString(MongoDbConnectorConfig.COLLECTION_SCHEMA_NAME_CONFIGURATION),
                    new TypeReference<List<MongodbSchemaNameConfig>>() {});
            if(schemaNameConfigs != null && !schemaNameConfigs.isEmpty()){
                for(MongodbSchemaNameConfig schemaNameConfig : schemaNameConfigs){
                    for(String collectionName : schemaNameConfig.getCollectionNames()) {
                        collectionSchemaNameMap.put(collectionName, schemaNameConfig.getSchemaName());
                    }
                }
                return;
            }
        } catch (IOException e) {
            logger.error("Error deserialize collection schema name config.", e);
        }
    }

    @Override
    public String getTopic(CollectionId collectionId) {
        StringJoiner sb = new StringJoiner(DELIMITER);
        if(prefix != null){
            sb.add(prefix);
        }
        sb.add(collectionId.dbName());
        String schemaName = collectionSchemaNameMap.get(collectionId.name());
        if(schemaName != null){
            sb.add(schemaName);
        }else {
            sb.add(collectionId.name());
        }
        return sb.toString();
    }
}
