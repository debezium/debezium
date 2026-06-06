/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link RawToStringConverter}, verifying that the configurable
 * charset property is used when decoding RAW bytes to strings.
 *
 * @author Bjorn Aangbaeck
 */
public class RawToStringConverterTest {

    @Test
    public void shouldDecodeRawBytesWithDefaultUtf8Charset() {
        final RawToStringConverter converter = new RawToStringConverter();
        converter.configure(new Properties());

        // "ÅÄÖ" in UTF-8: c3 85 c3 84 c3 96
        final byte[] utf8Bytes = new byte[]{ (byte) 0xc3, (byte) 0x85, (byte) 0xc3, (byte) 0x84, (byte) 0xc3, (byte) 0x96 };

        final Object result = invokeConverter(converter, utf8Bytes);
        assertThat(result).isEqualTo("\u00C5\u00C4\u00D6");
    }

    @Test
    public void shouldDecodeRawBytesWithLatin1Charset() {
        final Properties props = new Properties();
        props.setProperty("charset", "ISO-8859-1");

        final RawToStringConverter converter = new RawToStringConverter();
        converter.configure(props);

        // "ÅÄÖ" in Latin-1: c5 c4 d6
        final byte[] latin1Bytes = new byte[]{ (byte) 0xc5, (byte) 0xc4, (byte) 0xd6 };

        final Object result = invokeConverter(converter, latin1Bytes);
        assertThat(result).isEqualTo("\u00C5\u00C4\u00D6");
    }

    @Test
    public void shouldNotCorruptLatin1BytesWhenConfiguredCorrectly() {
        final Properties props = new Properties();
        props.setProperty("charset", "ISO-8859-1");

        final RawToStringConverter converter = new RawToStringConverter();
        converter.configure(props);

        // 0xC4 = Ä in Latin-1, but invalid as single UTF-8 byte
        final byte[] latin1Bytes = new byte[]{ (byte) 0xc4 };

        final Object result = invokeConverter(converter, latin1Bytes);
        assertThat(result).isEqualTo("\u00C4");
        assertThat(result).isNotEqualTo("\uFFFD");
    }

    @Test
    public void shouldUseUtf8ByDefaultForBackwardCompatibility() {
        final RawToStringConverter converter = new RawToStringConverter();
        converter.configure(new Properties());

        // Pure ASCII is identical in both charsets
        final byte[] asciiBytes = "HELLO".getBytes();

        final Object result = invokeConverter(converter, asciiBytes);
        assertThat(result).isEqualTo("HELLO");
    }

    private Object invokeConverter(RawToStringConverter converter, byte[] data) {
        final RelationalColumn column = mockRawColumn();
        final AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
        return converterRef.get().convert(data);
    }

    private RelationalColumn mockRawColumn() {
        final RelationalColumn column = Mockito.mock(RelationalColumn.class);
        Mockito.when(column.typeName()).thenReturn("RAW");
        Mockito.when(column.dataCollection()).thenReturn("TEST_TABLE");
        Mockito.when(column.name()).thenReturn("DATA");
        Mockito.when(column.isOptional()).thenReturn(true);
        return column;
    }
}
