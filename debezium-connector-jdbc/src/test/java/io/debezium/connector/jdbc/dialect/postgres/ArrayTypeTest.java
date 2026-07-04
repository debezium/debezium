/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the PostgreSQL {@link ArrayType} handler.
 */
@Tag("UnitTests")
class ArrayTypeTest {

    @Test
    @DisplayName("Should strip the precision/scale modifier for createArrayOf element type")
    void testStripsPrecisionAndScale() {
        // dbz#2100: numeric(10,2)[] previously passed "decimal(10,2)" to Connection#createArrayOf,
        // which the driver rejects with "Unable to find server array type for provided name decimal(10,2)".
        assertThat(ArrayType.baseElementTypeName("decimal(10,2)")).isEqualTo("decimal");
        assertThat(ArrayType.baseElementTypeName("numeric(10,2)")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("varchar(255)")).isEqualTo("varchar");
    }

    @Test
    @DisplayName("Should strip array brackets from the element type")
    void testStripsArrayBrackets() {
        assertThat(ArrayType.baseElementTypeName("numeric(10,2)[]")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("text[]")).isEqualTo("text");
        assertThat(ArrayType.baseElementTypeName("int[][]")).isEqualTo("int");
    }

    @Test
    @DisplayName("Should leave a bare base type name unchanged")
    void testLeavesBaseTypeUnchanged() {
        assertThat(ArrayType.baseElementTypeName("numeric")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("text")).isEqualTo("text");
        assertThat(ArrayType.baseElementTypeName("uuid")).isEqualTo("uuid");
    }

    @Test
    @DisplayName("Should lower-case the base type name")
    void testLowerCasesBaseType() {
        assertThat(ArrayType.baseElementTypeName("NUMERIC(10,2)")).isEqualTo("numeric");
        assertThat(ArrayType.baseElementTypeName("VARCHAR(255)[]")).isEqualTo("varchar");
    }
}
