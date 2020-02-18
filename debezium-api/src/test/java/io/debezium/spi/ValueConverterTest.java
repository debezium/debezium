/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi;

import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.fest.assertions.Assertions;
import org.junit.Test;

import io.debezium.spi.CustomConverter.ConverterDefinition;

/**
 * @author Jiri Pechanec
 */
public class ValueConverterTest {

    final CustomConverter<SchemaBuilder> testConverter = new CustomConverter<SchemaBuilder>() {
        private String field = "myfield";

        @Override
        public void configure(Properties props) {
            field = props.getProperty("field", field);
        }

        @Override
        public Optional<ConverterDefinition<SchemaBuilder>> converterFor(String fieldType, String fieldName,
                                                                         String dataCollectionName) {
            if (field.equals(fieldName)) {
                return Optional.of(new ConverterDefinition<>(SchemaBuilder.string().name("CUSTOM_STRING").optional(), (x) -> {
                    if (x instanceof Integer) {
                        return Integer.toString((Integer) x);
                    }
                    return x.toString();
                }));
            }
            return Optional.empty();
        }
    };

    @Test
    public void matchingField() {
        testConverter.configure(new Properties());
        final ConverterDefinition<SchemaBuilder> definition = testConverter.converterFor("VARCHAR2(30)", "myfield", "db1.table1").get();
        Assertions.assertThat(definition.fieldSchema.name()).isEqualTo("CUSTOM_STRING");
        Assertions.assertThat(definition.converter.convert(34)).isEqualTo("34");
    }

    @Test
    public void nonMatchingField() {
        testConverter.configure(new Properties());
        Assertions.assertThat(testConverter.converterFor("VARCHAR2(30)", "wrongfield", "db1.table1").isPresent()).isFalse();
    }

    @Test
    public void configuredField() {
        final Properties props = new Properties();
        props.setProperty("field", "otherfield");
        testConverter.configure(props);
        Assertions.assertThat(testConverter.converterFor("VARCHAR2(30)", "myfield", "db1.table1").isPresent()).isFalse();

        final ConverterDefinition<SchemaBuilder> definition = testConverter.converterFor("VARCHAR2(30)", "otherfield", "db1.table1").get();
        Assertions.assertThat(definition.fieldSchema.name()).isEqualTo("CUSTOM_STRING");
        Assertions.assertThat(definition.converter.convert(34)).isEqualTo("34");
    }
}
