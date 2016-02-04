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
    public void shouldWhitelistCommaSeparatedIntegers() {
        Predicate<Integer> p = Predicates.whitelist("1,2,3,4,5",',', Integer::parseInt);
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
    public void shouldBlacklistCommaSeparatedIntegers() {
        Predicate<Integer> p = Predicates.blacklist("1,2,3,4,5",',', Integer::parseInt);
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
