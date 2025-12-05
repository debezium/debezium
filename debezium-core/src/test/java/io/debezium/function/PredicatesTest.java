/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.function;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.function.Predicate;

import org.junit.Test;

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
        Predicate<Integer> p = Predicates.includes("1,2,3,4,5", (i) -> i.toString());
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
        Predicate<Integer> p = Predicates.excludes("1,2,3,4,5", (i) -> i.toString());
        assertThat(p.test(1)).isFalse();
        assertThat(p.test(2)).isFalse();
        assertThat(p.test(3)).isFalse();
        assertThat(p.test(4)).isFalse();
        assertThat(p.test(5)).isFalse();
        assertThat(p.test(0)).isTrue();
        assertThat(p.test(6)).isTrue();
        assertThat(p.test(-1)).isTrue();
    }

    @Test
    public void shouldMatchCommaSeparatedUuidLiterals() {
        String uuid1 = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        String uuid3 = UUID.randomUUID().toString();
        String uuid4 = UUID.randomUUID().toString();
        String uuid4Prefix = uuid4.substring(0, 10) + ".*";
        Predicate<String> p = Predicates.includesUuids(uuid1 + "," + uuid2);
        assertThat(p.test(uuid1)).isTrue();
        assertThat(p.test(uuid2)).isTrue();
        assertThat(p.test(uuid3)).isFalse();
        assertThat(p.test(uuid4)).isFalse();

        p = Predicates.excludesUuids(uuid1 + "," + uuid2);
        assertThat(p.test(uuid1)).isFalse();
        assertThat(p.test(uuid2)).isFalse();
        assertThat(p.test(uuid3)).isTrue();
        assertThat(p.test(uuid4)).isTrue();

        p = Predicates.includesUuids(uuid1 + "," + uuid2 + "," + uuid4Prefix);
        assertThat(p.test(uuid1)).isTrue();
        assertThat(p.test(uuid2)).isTrue();
        assertThat(p.test(uuid3)).isFalse();
        assertThat(p.test(uuid4)).isTrue();

        p = Predicates.excludesUuids(uuid1 + "," + uuid2 + "," + uuid4Prefix);
        assertThat(p.test(uuid1)).isFalse();
        assertThat(p.test(uuid2)).isFalse();
        assertThat(p.test(uuid3)).isTrue();
        assertThat(p.test(uuid4)).isFalse();

    }

}
