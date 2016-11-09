/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.relational.Table;

/**
 * @author Randall Hauch
 *
 */
public class ByTableTopicMapping implements TopicMappingProvider {

    @Override
    public TopicMapping getMapper(String prefix, Table table) {
        String topicName = prefix + table.id().toString();
        return new ByTableMapper(topicName);
    }
    
    protected static class ByTableMapper implements TopicMapping {
        private final String topicName;
        protected ByTableMapper(String topicName) {
            this.topicName = topicName;
        }
        
        @Override
        public String getTopicName() {
            return topicName;
        }
        
        @Override
        public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
            // do nothing ...
        }
        
        @Override
        public void addNonRowFieldsToKey(Schema schema, Struct rowBasedKey) {
            // do nothing ...
        }

    }

}
