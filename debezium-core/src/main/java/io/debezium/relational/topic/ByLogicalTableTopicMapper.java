/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.relational.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A logical table consists of one or more physical tables with the same schema. A common use case is sharding -- the
 * two physical tables `db_shard1.my_table` and `db_shard2.my_table` together form one logical table.
 *
 * This TopicMapper allows us to send change events from both physical tables to one topic.
 *
 * @author David Leibovic
 */
public class ByLogicalTableTopicMapper extends TopicMapper {

    public String getTopicName(String topicPrefix, Table table) {
        final String fullyQualifiedTableName = composeFullyQualifiedTableName(topicPrefix, table);
        Pattern logicalTablePattern = Pattern.compile("^.*?(?=\\..+\\..+)\\.(?<logicalDb>etsy_.+?(?=(_\\d+\\.)|\\.))(_\\d+)?\\.(?<table>.+)$");
        Matcher logicalTableMatcher = logicalTablePattern.matcher(fullyQualifiedTableName);
        if (logicalTableMatcher.matches()) {
            return logicalTableMatcher.replaceAll("${logicalDb}.${table}");
        }
        return fullyQualifiedTableName;
    }

    public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
        // Now that multiple physical tables can share a topic, the Key Schema can no longer consist of solely the
        // record's primary / unique key fields, since they are not guaranteed to be unique across tables. We need some
        // identifier added to the key that distinguishes the different physical tables.
        keySchemaBuilder.field("__dbz__physicalTableIdentifier", Schema.STRING_SCHEMA);
    }

    public Map<String, Object> getNonRowFieldsToAddToKey(Schema schema, String topicPrefix, Table table) {
        final String fullyQualifiedTableName = composeFullyQualifiedTableName(topicPrefix, table);
        Pattern physicalTableIdentifierPattern = Pattern.compile("^.*?(?=\\..+\\..+)\\.(?<logicalDb>etsy_.+?(?=\\.))\\.(?<table>.+)$");
        Matcher physicalTableIdentifierMatcher = physicalTableIdentifierPattern.matcher(fullyQualifiedTableName);

        final String physicalTableIdentifier;
        if (physicalTableIdentifierMatcher.matches()) {
            physicalTableIdentifier = physicalTableIdentifierMatcher.replaceAll("${logicalDb}");
        } else {
            physicalTableIdentifier = fullyQualifiedTableName;
        }

        Map<String, Object> nonRowFields = new HashMap<>();
        nonRowFields.put("__dbz__physicalTableIdentifier", physicalTableIdentifier);
        return nonRowFields;
    }

    private String composeFullyQualifiedTableName(String topicPrefix, Table table) {
        return topicPrefix + table.id().toString();
    }

}
