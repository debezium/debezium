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

import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.CustomConverter.ConverterDefinition;

/**
 * @author Jiri Pechanec
 */
public class ValueConverterTest {

    public static class BasicField implements ConvertedField {
        private final String name;
        private final String dataCollection;
        private final String type;

        public BasicField(String name, String dataCollection, String type) {
            super();
            this.name = name;
            this.dataCollection = dataCollection;
            this.type = type;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String dataCollection() {
            return dataCollection;
        }

        public String type() {
            return type;
        }
    }

    final CustomConverter<SchemaBuilder, BasicField> testConverter = new CustomConverter<SchemaBuilder, BasicField>() {
        private String convertedField = "myfield";

        @Override
        public void configure(Properties props) {
            convertedField = props.getProperty("field", convertedField);
        }

        @Override
        public Optional<ConverterDefinition<SchemaBuilder>> converterFor(BasicField field) {
            if (convertedField.equals(field.name())) {
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
        final ConverterDefinition<SchemaBuilder> definition = testConverter.converterFor(new BasicField("myfield", "db1.table1", "VARCHAR2(30)")).get();
        Assertions.assertThat(definition.fieldSchema.name()).isEqualTo("CUSTOM_STRING");
        Assertions.assertThat(definition.converter.convert(34)).isEqualTo("34");
    }

    @Test
    public void nonMatchingField() {
        testConverter.configure(new Properties());
        Assertions.assertThat(testConverter.converterFor(new BasicField("wrongfield", "db1.table1", "VARCHAR2(30)")).isPresent()).isFalse();
    }

    @Test
    public void configuredField() {
        final Properties props = new Properties();
        props.setProperty("field", "otherfield");
        testConverter.configure(props);
        Assertions.assertThat(testConverter.converterFor(new BasicField("myfield", "db1.table1", "VARCHAR2(30)")).isPresent()).isFalse();

        final ConverterDefinition<SchemaBuilder> definition = testConverter.converterFor(new BasicField("otherfield", "db1.table1", "VARCHAR2(30)")).get();
        Assertions.assertThat(definition.fieldSchema.name()).isEqualTo("CUSTOM_STRING");
        Assertions.assertThat(definition.converter.convert(34)).isEqualTo("34");
    }
}
