/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.converters;

import static junit.framework.TestCase.fail;
import static org.fest.assertions.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

public class ByteBufferConverterTest {

    private static final String TOPIC = "topic";
    private static final byte[] SAMPLE_BYTES = "sample string".getBytes(StandardCharsets.UTF_8);

    private ByteBufferConverter converter = new ByteBufferConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }

    @Test
    public void shouldConvertFromConnectData() {
        byte[] bytes = converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, ByteBuffer.wrap(SAMPLE_BYTES));

        assertThat(bytes).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertFromConnectDataForOptionalBytesSchema() {
        byte[] bytes = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BYTES_SCHEMA, ByteBuffer.wrap(SAMPLE_BYTES));

        assertThat(bytes).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertFromConnectDataWithoutSchema() {
        byte[] bytes = converter.fromConnectData(TOPIC, null, ByteBuffer.wrap(SAMPLE_BYTES));

        assertThat(bytes).isEqualTo(SAMPLE_BYTES);
    }

    @Test
    public void shouldConvertNullFromConnectData() {
        byte[] bytes = converter.fromConnectData(TOPIC, null, null);

        assertThat(bytes).isNull();
    }

    @Test
    public void shouldThrowWhenConvertNonByteSchemaFromConnectData() {
        try {
            converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, null);
            fail("now expected exception thrown");
        }
        catch (Exception e) {
            assertThat(e).isExactlyInstanceOf(DataException.class);
        }
    }

    @Test
    public void shouldThrowWhenConvertRawByteArrayFromConnectData() {
        try {
            converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, null);
            fail("now expected exception thrown");
        }
        catch (Exception e) {
            assertThat(e).isExactlyInstanceOf(DataException.class);
        }

    }

    @Test
    public void shouldConvertToConnectData() {
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, SAMPLE_BYTES);

        assertThat(schemaAndValue.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
        assertThat(schemaAndValue.value()).isEqualTo(ByteBuffer.wrap(SAMPLE_BYTES));
    }

    @Test
    public void shouldConvertToConnectDataForNullValue() {
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, null);

        assertThat(schemaAndValue.schema()).isEqualTo(Schema.OPTIONAL_BYTES_SCHEMA);
        assertThat(schemaAndValue.value()).isNull();
    }
}
