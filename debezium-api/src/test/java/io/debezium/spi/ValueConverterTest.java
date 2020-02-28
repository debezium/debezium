/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi;

import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.debezium.spi.converter.ConvertedField;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

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
        public void converterFor(BasicField field, ConverterRegistration<SchemaBuilder> registration) {
            if (convertedField.equals(field.name())) {
                registration.register(SchemaBuilder.string().name("CUSTOM_STRING").optional(), (x) -> {
                    if (x instanceof Integer) {
                        return Integer.toString((Integer) x);
                    }
                    return x.toString();
                });
            }
        }
    };

    private static class TestRegistration implements ConverterRegistration<SchemaBuilder> {
        public SchemaBuilder fieldSchema;
        public Converter converter;

        @Override
        public void register(SchemaBuilder fieldSchema, Converter converter) {
            this.fieldSchema = fieldSchema;
            this.converter = converter;
        }
    }

    private TestRegistration testRegistration;

    @Before
    public void before() {
        testRegistration = new TestRegistration();
    }

    @Test
    public void matchingField() {
        testConverter.configure(new Properties());
        testConverter.converterFor(new BasicField("myfield", "db1.table1", "VARCHAR2(30)"), testRegistration);
        Assertions.assertThat(testRegistration.fieldSchema.name()).isEqualTo("CUSTOM_STRING");
        Assertions.assertThat(testRegistration.converter.convert(34)).isEqualTo("34");
    }

    @Test
    public void nonMatchingField() {
        testConverter.configure(new Properties());
        testConverter.converterFor(new BasicField("wrongfield", "db1.table1", "VARCHAR2(30)"), testRegistration);
        Assertions.assertThat(testRegistration.fieldSchema).isNull();
    }

    @Test
    public void configuredField() {
        final Properties props = new Properties();
        props.setProperty("field", "otherfield");
        testConverter.configure(props);
        testConverter.converterFor(new BasicField("myfield", "db1.table1", "VARCHAR2(30)"), testRegistration);
        Assertions.assertThat(testRegistration.fieldSchema).isNull();

        testConverter.converterFor(new BasicField("otherfield", "db1.table1", "VARCHAR2(30)"), testRegistration);
        Assertions.assertThat(testRegistration.fieldSchema.name()).isEqualTo("CUSTOM_STRING");
        Assertions.assertThat(testRegistration.converter.convert(34)).isEqualTo("34");
    }
}
