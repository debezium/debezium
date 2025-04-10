/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.debezium.bindings.kafka.KafkaDebeziumSinkRecord;
import io.debezium.connector.jdbc.junit.jupiter.SinkRecordFactoryArgumentsProvider;
import io.debezium.connector.jdbc.util.NamingStyle;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.doc.FixFor;

/**
 * Tests for the CollectionNameTransformation.
 *
 * @author Chris Cranford
 */
public class CollectionNameTransformationTest {

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameDefaultStyle(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.INVENTORY_ONHAND_QUANTITIES", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("public.INVENTORY_ONHAND_QUANTITIES");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameDefaultStyleWithPrefixSuffix(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.prefix", "aa");
            properties.put("collection.naming.suffix", "bb");
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.INVENTORY_ONHAND_QUANTITIES", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("aapublic.INVENTORY_ONHAND_QUANTITIESbb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToSnakeCase(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.SNAKE_CASE.getValue());
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.inventoryOnhandQuantities", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("public_inventory_onhand_quantities");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToSnakeCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.SNAKE_CASE.getValue());
            properties.put("collection.naming.prefix", "aa");
            properties.put("collection.naming.suffix", "bb");
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.inventoryOnhandQuantities", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("aapublic_inventory_onhand_quantitiesbb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToCamelCase(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.CAMEL_CASE.getValue());
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.inventory_onhand_quantities", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("publicInventoryOnhandQuantities");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToCamelCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.CAMEL_CASE.getValue());
            properties.put("collection.naming.prefix", "aa");
            properties.put("collection.naming.suffix", "bb");
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.inventory_onhand_quantities", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("aapublicInventoryOnhandQuantitiesbb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToUpperCase(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.UPPER_CASE.getValue());
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.inventory_onhand_quantities", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("PUBLIC.INVENTORY_ONHAND_QUANTITIES");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToUpperCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.UPPER_CASE.getValue());
            properties.put("collection.naming.prefix", "aa");
            properties.put("collection.naming.suffix", "bb");
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.inventory_onhand_quantities", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("aaPUBLIC.INVENTORY_ONHAND_QUANTITIESbb");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToLowerCase(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.LOWER_CASE.getValue());
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.INVENTORY_ONHAND_QUANTITIES", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("public.inventory_onhand_quantities");
        }
    }

    @ParameterizedTest
    @ArgumentsSource(SinkRecordFactoryArgumentsProvider.class)
    @FixFor("DBZ-7051")
    void testConvertCollectionNameToLowerCaseWithPrefixSuffix(SinkRecordFactory factory) {
        try (CollectionNameTransformation<SinkRecord> transform = new CollectionNameTransformation<>()) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("collection.naming.style", NamingStyle.LOWER_CASE.getValue());
            properties.put("collection.naming.prefix", "aa");
            properties.put("collection.naming.suffix", "bb");
            transform.configure(properties);

            final KafkaDebeziumSinkRecord topicEvent = factory.createRecord("public.INVENTORY_ONHAND_QUANTITIES", (byte) 1);
            assertThat(transform.apply(topicEvent.getOriginalKafkaRecord()).topic()).isEqualTo("aapublic.inventory_onhand_quantitiesbb");
        }
    }
}
