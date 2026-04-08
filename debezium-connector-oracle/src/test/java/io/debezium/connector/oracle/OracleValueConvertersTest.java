/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.relational.Column;

import oracle.jdbc.OracleTypes;
import oracle.sql.CharacterSet;

/**
 * Unit tests for {@link OracleValueConverters}, specifically verifying that
 * HEXTORAW string decoding respects the database character set.
 *
 * @author Bjorn Aangbaeck
 */
public class OracleValueConvertersTest {

    private OracleValueConverters convertersUtf8;
    private OracleValueConverters convertersLatin1;

    @BeforeEach
    void setUp() {
        final Configuration configuration = TestHelper.defaultConfig().build();
        final OracleConnectorConfig connectorConfig = new OracleConnectorConfig(configuration);

        // Converter with AL32UTF8 database charset (common case)
        final OracleConnection utf8Connection = Mockito.mock(OracleConnection.class);
        Mockito.when(utf8Connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        Mockito.when(utf8Connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL32UTF8_CHARSET));
        convertersUtf8 = connectorConfig.getAdapter().getValueConverter(connectorConfig, utf8Connection);

        // Converter with WE8ISO8859P1 (Latin-1) database charset
        final OracleConnection latin1Connection = Mockito.mock(OracleConnection.class);
        Mockito.when(latin1Connection.getNationalCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.AL16UTF16_CHARSET));
        Mockito.when(latin1Connection.getDatabaseCharacterSet()).thenReturn(CharacterSet.make(CharacterSet.WE8ISO8859P1_CHARSET));
        convertersLatin1 = connectorConfig.getAdapter().getValueConverter(connectorConfig, latin1Connection);
    }

    @Test
    public void shouldDecodeHexToRawWithLatin1CharacterSet() {
        // "KESKUKSEN LISÄTARVIKE" in Latin-1 bytes
        // K=4b E=45 S=53 K=4b U=55 K=4b S=53 E=45 N=4e ' '=20 L=4c I=49 S=53 Ä=c4 T=54 A=41 R=52 V=56 I=49 K=4b E=45
        final String hexToRaw = "HEXTORAW('4b45534b554b53454e204c4953c454415256494b45')";

        final Column column = Column.editor()
                .name("VENDPARTDESCR1")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("VENDPARTDESCR1", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersLatin1.convertString(column, field, hexToRaw);
        assertThat(result).isEqualTo("KESKUKSEN LIS\u00C4TARVIKE");
    }

    @Test
    public void shouldDecodeHexToRawWithLatin1NordicCharacters() {
        // Test all Nordic characters: Å=c5 Ä=c4 Ö=d6 å=e5 ä=e4 ö=f6
        final String hexToRaw = "HEXTORAW('c5c4d6e5e4f6')";

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersLatin1.convertString(column, field, hexToRaw);
        assertThat(result).isEqualTo("\u00C5\u00C4\u00D6\u00E5\u00E4\u00F6");
    }

    @Test
    public void shouldDecodeHexToRawWithUtf8CharacterSet() {
        // "ÅÄÖ" in UTF-8 bytes: Å=c385 Ä=c384 Ö=c396
        final String hexToRaw = "HEXTORAW('c385c384c396')";

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersUtf8.convertString(column, field, hexToRaw);
        assertThat(result).isEqualTo("\u00C5\u00C4\u00D6");
    }

    @Test
    public void shouldDecodeHexToRawAsciiIdenticallyForBothCharsets() {
        // Pure ASCII is identical in both UTF-8 and Latin-1
        final String hexToRaw = "HEXTORAW('48454c4c4f')"; // "HELLO"

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        assertThat(convertersUtf8.convertString(column, field, hexToRaw)).isEqualTo("HELLO");
        assertThat(convertersLatin1.convertString(column, field, hexToRaw)).isEqualTo("HELLO");
    }

    @Test
    public void shouldNotCorruptLatin1BytesWhenDecodedAsLatin1() {
        // This is the exact bug scenario: byte 0xC4 (Ä in Latin-1) is NOT a valid
        // single-byte UTF-8 sequence. When decoded as UTF-8, it produces U+FFFD
        // (replacement character). When decoded as Latin-1, it correctly produces Ä.
        final String hexToRaw = "HEXTORAW('c4')";

        final Column column = Column.editor()
                .name("DATA")
                .type("VARCHAR2")
                .jdbcType(OracleTypes.VARCHAR)
                .create();

        final Field field = new Field("DATA", 0, SchemaBuilder.string().optional().build());

        final Object result = convertersLatin1.convertString(column, field, hexToRaw);

        // Must be Ä (U+00C4), NOT U+FFFD (replacement character)
        assertThat(result).isEqualTo("\u00C4");
        assertThat(result).isNotEqualTo("\uFFFD");
    }
}
