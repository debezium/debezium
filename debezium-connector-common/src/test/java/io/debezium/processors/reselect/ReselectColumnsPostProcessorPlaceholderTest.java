/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors.reselect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.bean.StandardBeanNames;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.doc.FixFor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Unit tests for how {@link ReselectColumnsPostProcessor} recognizes the unavailable value placeholder of array columns.
 */
public class ReselectColumnsPostProcessorPlaceholderTest {

    // 0x00 and a negative byte, both possible with a hex encoded placeholder
    private static final byte[] PLACEHOLDER = { 0x5f, 0x00, (byte) 0xff };

    private ReselectColumnsPostProcessor processor;

    @BeforeEach
    public void before() {
        final RelationalDatabaseConnectorConfig connectorConfig = mock(RelationalDatabaseConnectorConfig.class);
        when(connectorConfig.getUnavailableValuePlaceholder()).thenReturn(PLACEHOLDER);
        final BeanRegistry beanRegistry = mock(BeanRegistry.class);
        when(beanRegistry.lookupByName(StandardBeanNames.CONNECTOR_CONFIG, RelationalDatabaseConnectorConfig.class)).thenReturn(connectorConfig);

        processor = new ReselectColumnsPostProcessor();
        processor.injectBeanRegistry(beanRegistry);
    }

    @Test
    @FixFor("debezium/dbz#2418")
    public void shouldRecognizeNumericPlaceholderPerByte() {
        assertPlaceholder(Schema.OPTIONAL_INT16_SCHEMA, List.of((short) 95, (short) 0, (short) -1), List.of((short) 95, (short) 0, (short) 1));
        assertPlaceholder(Schema.OPTIONAL_INT32_SCHEMA, List.of(95, 0, -1), List.of(95, 0, 1));
        assertThat(processor.isUnavailableValueHolder(arrayField(Schema.OPTIONAL_INT32_SCHEMA), List.of(95, 0))).as("shorter list").isFalse();
        assertPlaceholder(Schema.OPTIONAL_INT64_SCHEMA, List.of(95L, 0L, -1L), List.of(95L, 0L, 1L));
        assertPlaceholder(Schema.OPTIONAL_FLOAT32_SCHEMA, List.of(95f, 0f, -1f), List.of(95f, 0f, 1f));
        assertPlaceholder(Schema.OPTIONAL_FLOAT64_SCHEMA, List.of(95.0, 0.0, -1.0), List.of(95.0, 0.0, 1.0));
        assertPlaceholder(Decimal.builder(2).optional().build(),
                List.of(new BigDecimal("95.00"), new BigDecimal("0.00"), new BigDecimal("-1.00")),
                List.of(new BigDecimal("0.95"), new BigDecimal("0.00"), new BigDecimal("-0.01")));
        final Schema variableScale = VariableScaleDecimal.optionalSchema();
        assertPlaceholder(variableScale,
                List.of(VariableScaleDecimal.fromLogical(variableScale, BigDecimal.valueOf(95)),
                        VariableScaleDecimal.fromLogical(variableScale, BigDecimal.ZERO),
                        VariableScaleDecimal.fromLogical(variableScale, BigDecimal.valueOf(-1))),
                List.of(VariableScaleDecimal.fromLogical(variableScale, BigDecimal.valueOf(95)),
                        VariableScaleDecimal.fromLogical(variableScale, BigDecimal.ZERO),
                        VariableScaleDecimal.fromLogical(variableScale, BigDecimal.ONE)));
    }

    @Test
    @FixFor("debezium/dbz#2418")
    public void shouldRecognizePlaceholderAsSingleElement() {
        assertPlaceholder(Schema.OPTIONAL_BYTES_SCHEMA, List.of(PLACEHOLDER), List.of(new byte[]{ 0x5f }));
        assertPlaceholder(Schema.OPTIONAL_STRING_SCHEMA, List.of(new String(PLACEHOLDER)), List.of("value"));
    }

    private void assertPlaceholder(Schema elementSchema, List<?> placeholder, List<?> data) {
        final Field field = arrayField(elementSchema);
        assertThat(processor.isUnavailableValueHolder(field, placeholder)).as("placeholder of %s", elementSchema).isTrue();
        assertThat(processor.isUnavailableValueHolder(field, data)).as("data of %s", elementSchema).isFalse();
    }

    private static Field arrayField(Schema elementSchema) {
        return new Field("array", 0, SchemaBuilder.array(elementSchema).optional().build());
    }
}
