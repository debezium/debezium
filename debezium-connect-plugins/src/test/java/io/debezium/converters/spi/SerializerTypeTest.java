/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.converters.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for {@link SerializerType}.
 */
public class SerializerTypeTest {

    @Test
    @FixFor("debezium/dbz#2737")
    public void shouldReturnSerializerTypeForValidNames() {
        assertThat(SerializerType.withName("json")).isEqualTo(SerializerType.JSON);
        assertThat(SerializerType.withName("JSON")).isEqualTo(SerializerType.JSON);
        assertThat(SerializerType.withName("Json")).isEqualTo(SerializerType.JSON);
        assertThat(SerializerType.withName("avro")).isEqualTo(SerializerType.AVRO);
        assertThat(SerializerType.withName("AVRO")).isEqualTo(SerializerType.AVRO);
        assertThat(SerializerType.withName("Avro")).isEqualTo(SerializerType.AVRO);
    }

    @Test
    @FixFor("debezium/dbz#2737")
    public void shouldReturnNullForNullOrInvalidNames() {
        assertThat(SerializerType.withName(null)).isNull();
        assertThat(SerializerType.withName("")).isNull();
        assertThat(SerializerType.withName("unknown")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2737")
    public void shouldLookupSerializerTypeWithSpecialCasingLocales() {
        Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            assertThat(SerializerType.withName("json")).isEqualTo(SerializerType.JSON);
            assertThat(SerializerType.withName("JSON")).isEqualTo(SerializerType.JSON);
            assertThat(SerializerType.withName("avro")).isEqualTo(SerializerType.AVRO);
            assertThat(SerializerType.withName("AVRO")).isEqualTo(SerializerType.AVRO);

            Locale.setDefault(new Locale("az", "AZ"));
            assertThat(SerializerType.withName("json")).isEqualTo(SerializerType.JSON);
            assertThat(SerializerType.withName("JSON")).isEqualTo(SerializerType.JSON);
        }
        finally {
            Locale.setDefault(defaultLocale);
        }
    }
}
