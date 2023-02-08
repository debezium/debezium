/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.schema.SchemaNameAdjuster.ReplacementOccurred;
import io.debezium.schema.UnicodeReplacementFunction;
import io.debezium.spi.common.ReplacementFunction;

/**
 * @author Randall Hauch
 *
 */
public class SchemaNameAdjusterTest {

    private ReplacementFunction underscoreReplacement;
    private ReplacementFunction unicodeReplacement;

    @Before
    public void before() {
        underscoreReplacement = ReplacementFunction.UNDERSCORE_REPLACEMENT;
        unicodeReplacement = new UnicodeReplacementFunction();
    }

    @Test
    public void shouldDetermineValidFirstCharacters() {
        String validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        for (int i = 0; i != validChars.length(); ++i) {
            assertThat(underscoreReplacement.isValidFirstCharacter(validChars.charAt(i))).isTrue();
        }
    }

    @Test
    public void shouldDetermineValidNonFirstCharacters() {
        String validChars = ".abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
        for (int i = 0; i != validChars.length(); ++i) {
            assertThat(underscoreReplacement.isValidNonFirstCharacter(validChars.charAt(i))).isTrue();
        }
    }

    @Test
    public void shouldConsiderValidFullnames() {
        assertValidFullname("test_server.connector_test.products.Key");
        assertValidFullname("t1234.connector_test.products.Key");
    }

    @Test
    public void shouldConsiderInvalidFullnames() {
        assertNotValidFullname("test-server.connector_test.products.Key");
    }

    @Test
    public void shouldConsiderInvalidFirstCharacters() {
        String invalidChars = "1_语言";
        for (int i = 0; i < invalidChars.length(); i++) {
            assertThat(unicodeReplacement.isValidFirstCharacter(invalidChars.charAt(i))).isFalse();
        }

        invalidChars = "_语言";
        for (int i = 0; i < invalidChars.length(); i++) {
            assertThat(unicodeReplacement.isValidNonFirstCharacter(invalidChars.charAt(i))).isFalse();
        }
    }

    @Test
    public void shouldConvertInvalidCharactersToUnicode() {
        SchemaNameAdjuster unicodeAdjuster = SchemaNameAdjuster.AVRO_UNICODE;
        String originName = "_hello_语言.";
        String expectedName = "_u005fhello_u005f_u8bed_u8a00.";
        assertThat(unicodeAdjuster.adjust(originName)).isEqualTo(expectedName);
    }

    @Test
    public void shouldReportReplacementEveryTime() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null) {
                conflicts.incrementAndGet();
            }
            counter.incrementAndGet();
        };
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.create("_", handler);
        for (int i = 0; i != 20; ++i) {
            adjuster.adjust("some-invalid-fullname$");
        }
        assertThat(counter.get()).isEqualTo(20);
        assertThat(conflicts.get()).isEqualTo(0);
    }

    @Test
    public void shouldReportReplacementOnlyOnce() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null) {
                conflicts.incrementAndGet();
            }
            counter.incrementAndGet();
        };
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.create("_", handler.firstTimeOnly());
        for (int i = 0; i != 20; ++i) {
            adjuster.adjust("some-invalid-fullname$");
        }
        assertThat(counter.get()).isEqualTo(1);
        assertThat(conflicts.get()).isEqualTo(0);
    }

    @Test
    public void shouldReportConflictReplacement() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null) {
                conflicts.incrementAndGet();
            }
            counter.incrementAndGet();
        };
        SchemaNameAdjuster adjuster = SchemaNameAdjuster.create("_", handler.firstTimeOnly());
        adjuster.adjust("some-invalid-fullname$");
        adjuster.adjust("some-invalid%fullname_");
        assertThat(counter.get()).isEqualTo(2);
        assertThat(conflicts.get()).isEqualTo(1);
    }

    protected void assertValidFullname(String fullname) {
        assertThat(SchemaNameAdjuster.isValidFullname(fullname)).isTrue();
    }

    protected void assertNotValidFullname(String fullname) {
        assertThat(SchemaNameAdjuster.isValidFullname(fullname)).isFalse();
    }
}
