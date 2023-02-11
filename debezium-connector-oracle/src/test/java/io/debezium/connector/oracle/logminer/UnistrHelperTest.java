/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.doc.FixFor;

/**
 * Unit tests for parsing {@code UNISTR} column values
 *
 * @author Chris Cranford
 */
public class UnistrHelperTest {
    @Test
    @FixFor("DBZ-3892")
    public void shouldCorrectlyDetectIfUnistrValue() throws Exception {
        assertThat(UnistrHelper.isUnistrFunction("UNISTR('\\0412\\044B')")).isTrue();
        assertThat(UnistrHelper.isUnistrFunction("OTHER_FUNCTION('\\0412\\044B')")).isFalse();
    }

    @Test
    @FixFor("DBZ-3892")
    public void shouldConvertUnistrValues() throws Exception {
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')")).isEqualTo("Вы");

        // Concatenated values, using differing spacing techniques to validate behavior
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B') || UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')||UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B') ||UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')|| UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')||  UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
        assertThat(UnistrHelper.convert("UNISTR('\\0412\\044B')  ||  UNISTR('\\0431\\0443')")).isEqualTo("Выбу");
    }
}
