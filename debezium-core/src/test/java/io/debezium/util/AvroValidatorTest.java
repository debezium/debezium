/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import io.debezium.util.AvroValidator.ReplacementOccurred;

/**
 * @author Randall Hauch
 *
 */
public class AvroValidatorTest {

    @Test
    public void shouldDetermineValidFirstCharacters() {
        String validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        for (int i = 0; i != validChars.length(); ++i) {
            assertThat(AvroValidator.isValidFullnameFirstCharacter(validChars.charAt(i))).isTrue();
        }
    }

    @Test
    public void shouldDetermineValidNonFirstCharacters() {
        String validChars = ".abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
        for (int i = 0; i != validChars.length(); ++i) {
            assertThat(AvroValidator.isValidFullnameNonFirstCharacter(validChars.charAt(i))).isTrue();
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
    public void shouldReportReplacementEveryTime() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null) conflicts.incrementAndGet();
            counter.incrementAndGet();
        };
        AvroValidator validator = AvroValidator.create(handler);
        for (int i = 0; i != 20; ++i) {
            validator.validate("some-invalid-fullname$");
        }
        assertThat(counter.get()).isEqualTo(20);
        assertThat(conflicts.get()).isEqualTo(0);
    }

    @Test
    public void shouldReportReplacementOnlyOnce() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null) conflicts.incrementAndGet();
            counter.incrementAndGet();
        };
        AvroValidator validator = AvroValidator.create(handler.firstTimeOnly());
        for (int i = 0; i != 20; ++i) {
            validator.validate("some-invalid-fullname$");
        }
        assertThat(counter.get()).isEqualTo(1);
        assertThat(conflicts.get()).isEqualTo(0);
    }

    @Test
    public void shouldReportConflictReplacement() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null) conflicts.incrementAndGet();
            counter.incrementAndGet();
        };
        AvroValidator validator = AvroValidator.create(handler.firstTimeOnly());
        validator.validate("some-invalid-fullname$");
        validator.validate("some-invalid%fullname_");
        assertThat(counter.get()).isEqualTo(2);
        assertThat(conflicts.get()).isEqualTo(1);
    }

    protected void assertValidFullname(String fullname) {
        assertThat(AvroValidator.isValidFullname(fullname)).isTrue();
    }

    protected void assertNotValidFullname(String fullname) {
        assertThat(AvroValidator.isValidFullname(fullname)).isFalse();
    }

}
