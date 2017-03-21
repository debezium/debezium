/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;
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

    // "^.*?(?=\\..+\\..+)\\.(?<logicalDb>etsy_.+?(?=(_\\d+\\.)|\\.))(_\\d+)?\\.(?<table>.+)$"
    private static final Field LOGICAL_TABLE_REGEX = Field.create("logical.table.regex")
            .withDisplayName("Logical table regex")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isRegex)
            .withDescription("The tables for which changes are to be captured");

    // "${logicalDb}.${table}"
    private static final Field LOGICAL_TABLE_REPLACEMENT = Field.create("logical.table.replacement")
            .withDisplayName("Logical table replacement")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The tables for which changes are to be captured");

    // "^.*?(?=\\..+\\..+)\\.(?<logicalDb>etsy_.+?(?=\\.))\\.(?<table>.+)$"
    private static final Field PHYSICAL_TABLE_REGEX = Field.create("physical.table.regex")
            .withDisplayName("Physical table regex")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isRegex)
            .withDescription("The tables for which changes are to be captured");

    // "${logicalDb}"
    private static final Field PHYSICAL_TABLE_REPLACEMENT = Field.create("physical.table.replacement")
            .withDisplayName("Physical table replacement")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The tables for which changes are to be captured");

    public Field.Set configFields() {
        return Field.setOf(LOGICAL_TABLE_REGEX, PHYSICAL_TABLE_REGEX);
    }

    public String getTopicName(String topicPrefix, Table table) {
        final String fullyQualifiedTableName = composeFullyQualifiedTableName(topicPrefix, table);
        Pattern logicalTablePattern = Pattern.compile(config.getString(LOGICAL_TABLE_REGEX));
        Matcher logicalTableMatcher = logicalTablePattern.matcher(fullyQualifiedTableName);
        if (logicalTableMatcher.matches()) {
            return logicalTableMatcher.replaceAll(config.getString(LOGICAL_TABLE_REPLACEMENT));
        }
        return null;
    }

    public void enhanceKeySchema(SchemaBuilder keySchemaBuilder) {
        // Now that multiple physical tables can share a topic, the Key Schema can no longer consist of solely the
        // record's primary / unique key fields, since they are not guaranteed to be unique across tables. We need some
        // identifier added to the key that distinguishes the different physical tables.
        keySchemaBuilder.field("__dbz__physicalTableIdentifier", Schema.STRING_SCHEMA);
    }

    public Map<String, Object> getNonRowFieldsToAddToKey(Schema schema, String topicPrefix, Table table) {
        final String fullyQualifiedTableName = composeFullyQualifiedTableName(topicPrefix, table);
        Pattern physicalTableIdentifierPattern = Pattern.compile(config.getString(PHYSICAL_TABLE_REGEX));
        Matcher physicalTableIdentifierMatcher = physicalTableIdentifierPattern.matcher(fullyQualifiedTableName);

        final String physicalTableIdentifier;
        if (physicalTableIdentifierMatcher.matches()) {
            physicalTableIdentifier = physicalTableIdentifierMatcher.replaceAll(config.getString(PHYSICAL_TABLE_REPLACEMENT));
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
