/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import java.util.function.Predicate;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 */
public class PredicatesTest {

    @Test
    public void shouldMatchCommaSeparatedRegexIncludes() {
        Predicate<String> p = Predicates.includes("1.*5,30");
        assertThat(p.test("30")).isTrue();
        assertThat(p.test("1005")).isTrue();
        assertThat(p.test("105")).isTrue();
        assertThat(p.test("15")).isTrue();
        assertThat(p.test("215")).isFalse();
        assertThat(p.test("150")).isFalse();
        assertThat(p.test("015")).isFalse();
        assertThat(p.test("5")).isFalse();
    }

    @Test
    public void shouldMatchCommaSeparatedLiteralIncludes() {
        Predicate<Integer> p = Predicates.includes("1,2,3,4,5", (i)->i.toString());
        assertThat(p.test(1)).isTrue();
        assertThat(p.test(2)).isTrue();
        assertThat(p.test(3)).isTrue();
        assertThat(p.test(4)).isTrue();
        assertThat(p.test(5)).isTrue();
        assertThat(p.test(0)).isFalse();
        assertThat(p.test(6)).isFalse();
        assertThat(p.test(-1)).isFalse();
    }

    @Test
    public void shouldMatchCommaSeparatedLiteralExcludes() {
        Predicate<Integer> p = Predicates.excludes("1,2,3,4,5", (i)->i.toString());
        assertThat(p.test(1)).isFalse();
        assertThat(p.test(2)).isFalse();
        assertThat(p.test(3)).isFalse();
        assertThat(p.test(4)).isFalse();
        assertThat(p.test(5)).isFalse();
        assertThat(p.test(0)).isTrue();
        assertThat(p.test(6)).isTrue();
        assertThat(p.test(-1)).isTrue();
    }

}
