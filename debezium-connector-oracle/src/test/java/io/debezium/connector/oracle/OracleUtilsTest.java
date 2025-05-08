/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.oracle.util.OracleUtils;
import io.debezium.doc.FixFor;

/**
 * Unit tests for the {@link OracleUtils} utility helper class.
 *
 * @author Chris Cranford
 */
public class OracleUtilsTest {

    @Test
    @FixFor("DBZ-9013")
    public void shouldReturnObjectNameNullOrEmpty() {
        assertThat(OracleUtils.isObjectNameNullOrEmpty(null)).isTrue();
        assertThat(OracleUtils.isObjectNameNullOrEmpty("")).isTrue();
        assertThat(OracleUtils.isObjectNameNullOrEmpty("\"\"")).isTrue();
    }

    @Test
    @FixFor("DBZ-9013")
    public void shouldReturnObjectNameIsNotNullOrEmpty() {
        assertThat(OracleUtils.isObjectNameNullOrEmpty("\"abc\"")).isFalse();
        assertThat(OracleUtils.isObjectNameNullOrEmpty("abc")).isFalse();
    }

    @Test
    @FixFor("DBZ-9013")
    public void shouldReturnObjectNameWithoutAnyChanges() {
        assertThat(OracleUtils.getObjectName("\"AbCdEfG\"")).isEqualTo("\"AbCdEfG\"");
    }

    @Test
    @FixFor("DBZ-9013")
    public void shouldReturnObjectNameInUppercase() {
        assertThat(OracleUtils.getObjectName("AbCdEfG")).isEqualTo("ABCDEFG");
    }

}
