/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.starrocks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.type.SqlTypes.BIGINT;
import static org.hibernate.type.SqlTypes.BLOB;
import static org.hibernate.type.SqlTypes.BOOLEAN;
import static org.hibernate.type.SqlTypes.CLOB;
import static org.hibernate.type.SqlTypes.DOUBLE;
import static org.hibernate.type.SqlTypes.FLOAT;
import static org.hibernate.type.SqlTypes.INTEGER;
import static org.hibernate.type.SqlTypes.NCLOB;
import static org.hibernate.type.SqlTypes.NVARCHAR;
import static org.hibernate.type.SqlTypes.REAL;
import static org.hibernate.type.SqlTypes.TIME;
import static org.hibernate.type.SqlTypes.TIMESTAMP;
import static org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.TIME_WITH_TIMEZONE;
import static org.hibernate.type.SqlTypes.VARCHAR;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests that pin the StarRocks-specific column type mappings of {@link StarRocksDialect},
 * guarding against behavior changes inherited from Hibernate's {@code MySQLDialect}.
 */
@Tag("UnitTests")
class StarRocksDialectTest {

    private static class TestableStarRocksDialect extends StarRocksDialect {
        String resolveColumnType(int sqlTypeCode) {
            return columnType(sqlTypeCode);
        }
    }

    private final TestableStarRocksDialect dialect = new TestableStarRocksDialect();

    @Test
    @DisplayName("Should map BOOLEAN to boolean rather than MySQL's bit")
    void testBooleanColumnType() {
        assertThat(dialect.resolveColumnType(BOOLEAN)).isEqualTo("boolean");
    }

    @Test
    @DisplayName("Should map timestamp types to datetime without precision")
    void testTimestampColumnTypes() {
        // StarRocks rejects a precision argument on DATETIME; the bare type retains
        // microsecond precision from v3.3.5 onward.
        assertThat(dialect.resolveColumnType(TIMESTAMP)).isEqualTo("datetime");
        assertThat(dialect.resolveColumnType(TIMESTAMP_WITH_TIMEZONE)).isEqualTo("datetime");
    }

    @Test
    @DisplayName("Should map time types to varchar as StarRocks has no TIME type")
    void testTimeColumnTypes() {
        assertThat(dialect.resolveColumnType(TIME)).isEqualTo("varchar(18)");
        assertThat(dialect.resolveColumnType(TIME_WITH_TIMEZONE)).isEqualTo("varchar(18)");
    }

    @Test
    @DisplayName("Should map large character types to maximum-length varchar rather than MySQL's longtext")
    void testLargeCharacterColumnTypes() {
        // The STRING alias only covers 65533 bytes; a maximum-length VARCHAR is larger.
        assertThat(dialect.resolveColumnType(CLOB)).isEqualTo("varchar(1048576)");
        assertThat(dialect.resolveColumnType(NCLOB)).isEqualTo("varchar(1048576)");
    }

    @Test
    @DisplayName("Should map BLOB to varbinary rather than MySQL's longblob")
    void testBlobColumnType() {
        assertThat(dialect.resolveColumnType(BLOB)).isEqualTo("varbinary(1048576)");
    }

    @Test
    @DisplayName("Should not use MySQL's deprecated character set clause for nationalized types")
    void testNationalizedColumnTypes() {
        assertThat(dialect.resolveColumnType(NVARCHAR)).isEqualTo(dialect.resolveColumnType(VARCHAR));
    }

    @Test
    @DisplayName("Should retain MySQL-compatible integer mappings")
    void testIntegerColumnTypes() {
        assertThat(dialect.resolveColumnType(INTEGER)).isEqualTo("integer");
        assertThat(dialect.resolveColumnType(BIGINT)).isEqualTo("bigint");
    }

    @Test
    @DisplayName("Should map floating point types without precision arguments")
    void testFloatingPointColumnTypes() {
        // StarRocks rejects float(p) and the "double precision" spelling.
        assertThat(dialect.resolveColumnType(FLOAT)).isEqualTo("float");
        assertThat(dialect.resolveColumnType(REAL)).isEqualTo("float");
        assertThat(dialect.resolveColumnType(DOUBLE)).isEqualTo("double");
    }

    @Test
    @DisplayName("Should limit VARCHAR to StarRocks byte-length maximum")
    void testMaxVarcharLength() {
        assertThat(dialect.getMaxVarcharLength()).isEqualTo(1_048_576);
        assertThat(dialect.getMaxVarbinaryLength()).isEqualTo(1_048_576);
    }

    @Test
    @DisplayName("Should limit DECIMAL precision to StarRocks maximum of 38")
    void testDefaultDecimalPrecision() {
        assertThat(dialect.getDefaultDecimalPrecision()).isEqualTo(38);
    }
}
