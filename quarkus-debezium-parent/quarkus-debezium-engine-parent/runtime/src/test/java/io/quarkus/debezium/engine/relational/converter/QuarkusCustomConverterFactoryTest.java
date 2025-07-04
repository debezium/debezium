/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.relational.converter;

import static org.apache.kafka.connect.data.Schema.Type.INT8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.CustomConverterRegistry;
import io.debezium.spi.converter.ConvertedField;

class QuarkusCustomConverterFactoryTest {

    private final static ConvertedField FIELD = new ConvertedField() {
        @Override
        public String name() {
            return "";
        }

        @Override
        public String dataCollection() {
            return "";
        }
    };
    private final QuarkusCustomConverter disableConverter = Mockito.mock(QuarkusCustomConverter.class);
    private final QuarkusCustomConverter enabledConverter = Mockito.mock(QuarkusCustomConverter.class);

    private final QuarkusCustomConverterFactory underTest = new QuarkusCustomConverterFactory(List.of(
            disableConverter,
            enabledConverter));

    @Test
    @DisplayName("should create converter only for enable converter")
    void shouldCreateConverterOnlyForEnabledConverter() {
        CustomConverterRegistry.ConverterDefinition<SchemaBuilder> enabled = new CustomConverterRegistry.ConverterDefinition<>(new SchemaBuilder(INT8), null);

        when(enabledConverter.filter(FIELD)).thenReturn(true);
        when(enabledConverter.bind(FIELD)).thenReturn(enabled);
        when(disableConverter.filter(FIELD)).thenReturn(false);

        List<SchemaBuilder> actual = new ArrayList<>();

        underTest.get().converterFor(FIELD, (fieldSchema, converter) -> actual.add(fieldSchema));

        assertThat(actual).containsExactlyInAnyOrder(enabled.fieldSchema);

        verify(disableConverter, times(0)).bind(FIELD);
    }
}
