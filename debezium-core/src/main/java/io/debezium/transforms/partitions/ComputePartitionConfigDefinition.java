/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.transforms.partitions;

import static io.debezium.transforms.partitions.ComputePartitionConfigDefinition.FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

public class ComputePartitionConfigDefinition {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputePartitionConfigDefinition.class);

    public static final String MAPPING_SEPARATOR = ":";
    public static final String LIST_SEPARATOR = ",";

    private ComputePartitionConfigDefinition() { // intentionally blank
    }

    public static final String FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF = "partition.data-collections.field.mappings";
    public static final String FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF = "partition.data-collections.partition.num.mappings";
    static final Field PARTITION_TABLE_FIELD_NAME_MAPPINGS_FIELD = Field.create(FIELD_TABLE_FIELD_NAME_MAPPINGS_CONF)
            .withDisplayName("Data collection field mapping")
            .withType(ConfigDef.Type.STRING)
            .withValidation(ComputePartitionConfigDefinition::isValidMapping)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of colon-delimited data collection field pairs, e.g. inventory.products:name,inventory.orders:purchaser");

    static final Field FIELD_TABLE_PARTITION_NUM_MAPPINGS_FIELD = Field.create(FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF)
            .withDisplayName("Data collection number of partition mapping")
            .withType(ConfigDef.Type.STRING)
            .withValidation(ComputePartitionConfigDefinition::isValidMapping)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Comma-separated list of colon-delimited data-collections partition number pairs, e.g. inventory.products:2,inventory.orders:3");

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
        }
        catch (Exception e) {
            LOGGER.error(String.format("Error while parsing values %s", values), e);
            problems.accept(field, values, "Problem parsing list of colon-delimited pairs, e.g. <code>foo:bar,abc:xyz</code>");
            return 1;
        }
        return 0;
    }

    static Map<String, Integer> parseParititionMappings(List<String> mappings) {

        final Map<String, Integer> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(MAPPING_SEPARATOR);
            if (parts.length != 2) {
                throw new ComputePartitionException("Invalid mapping: " + mapping);
            }
            try {
                final int value = Integer.parseInt(parts[1]);
                if (value <= 0) {
                    throw new ComputePartitionException(
                            String.format("Unable to validate config. %s: partition number for '%s' must be positive",
                                    FIELD_TABLE_PARTITION_NUM_MAPPINGS_CONF, parts[0]));
                }
                m.put(parts[0], value);
            }
            catch (NumberFormatException e) {
                throw new ComputePartitionException(String.format("Invalid mapping value: %s", parts[1]), e);
            }
        }
        return m;
    }
}
