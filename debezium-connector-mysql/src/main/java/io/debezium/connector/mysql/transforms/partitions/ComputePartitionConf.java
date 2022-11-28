/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.transforms.partitions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

public class ComputePartitionConf {

    public static final String MAPPING_SEPARATOR = ":";
    public static final String LIST_SEPARATOR = ",";

    private ComputePartitionConf() { // intentionally blank
    }

    public static final String FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF = "partition.table.field.mappings";
    public static final String FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF = "partition.table.partition.num.mappings";
    public static final String FIELD_TABLE_LIST_CONF = "partition.table.list";

    static final Field PARTITION_TABLE_LIST_FIELD = Field.create(FIELD_TABLE_LIST_CONF)
            .withDisplayName("List of the tables that activate the SMT")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma separated list of table names that will be considered for the transformation");

    static final Field PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD = Field.create(FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF)
            .withDisplayName("Table field mapping")
            .withType(ConfigDef.Type.STRING)
            .withValidation(ComputePartitionConf::isValidMapping)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("comma-separated list of colon-delimited table field pairs, e.g. products:name,orders:purchaser");

    static final Field FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD = Field.create(FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF)
            .withDisplayName("Table number of  partition mapping")
            .withType(ConfigDef.Type.STRING)
            .withValidation(ComputePartitionConf::isValidMapping)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("comma-separated list of colon-delimited table partition number pairs, e.g. products:2,orders:3");

    static Map<String, String> parseMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(MAPPING_SEPARATOR);
            if (parts.length != 2) {
                throw new ComputePartitionException("Invalid mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }

    public static int isValidMapping(Configuration config, Field field, Field.ValidationOutput problems) {
        List<String> values = config.getStrings(field, LIST_SEPARATOR);
        try {
            parseMappings(values);
            return 0;
        }
        catch (Exception e) {
        }
        problems.accept(field, values, "list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>");
        return 1;
    }

    static Map<String, Integer> parseIntMappings(List<String> mappings) {
        final Map<String, Integer> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ComputePartitionException("Invalid mapping: " + mapping);
            }
            try {
                int value = Integer.parseInt(parts[1]);
                m.put(parts[0], value);
            }
            catch (NumberFormatException e) {
                throw new ComputePartitionException("Invalid mapping value: " + e.getMessage());
            }
        }
        return m;
    }
}
