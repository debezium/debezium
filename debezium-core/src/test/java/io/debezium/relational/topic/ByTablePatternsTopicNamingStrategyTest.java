/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import java.util.function.Function;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class ByTablePatternsTopicNamingStrategyTest {

    private Function<String,String> func;
    private ByTablePatternsTopicNamingStrategy s;
    
    @Test
    public void shouldBuildFunctionFromOneRegexAndReplacement() {
        func = ByTablePatternsTopicNamingStrategy.mapRegex("^(\\w*?)(?=(?:_(?:\\d+)|$)+$)(_(?:21|22|51|52|91))?.*", "$1$2");
        assertThat(func.apply("ins_user_21_201601")).isEqualTo("ins_user_21");
        assertThat(func.apply("ins_user_21")).isEqualTo("ins_user_21");
        assertThat(func.apply("ins_user")).isEqualTo("ins_user");
        assertThat(func.apply("ins")).isEqualTo("ins");
    }
    
    @Test
    public void shouldBuildFunctionFromRegexAndReplacementPair() {
        func = ByTablePatternsTopicNamingStrategy.mapRegex("^(\\w*?)(?=(?:_(?:\\d+)|$)+$)(_(?:21|22|51|52|91))?.* =  $1$2");
        assertThat(func.apply("ins_user_21_201601")).isEqualTo("ins_user_21");
        assertThat(func.apply("ins_user_21")).isEqualTo("ins_user_21");
        assertThat(func.apply("ins_user")).isEqualTo("ins_user");
        assertThat(func.apply("ins")).isEqualTo("ins");

        func = ByTablePatternsTopicNamingStrategy.mapRegex("(\\d+)(\\w+)=$1");
        assertThat(func.apply("123")).isEqualTo("12");
        assertThat(func.apply("123abc")).isEqualTo("123");
        assertThat(func.apply("ins")).isNull();

        func = ByTablePatternsTopicNamingStrategy.mapRegex("(\\w+) =  $1");
        assertThat(func.apply("ins_user_21")).isEqualTo("ins_user_21");
        assertThat(func.apply("ins_user_21")).isEqualTo("ins_user_21");
        assertThat(func.apply("ins_user")).isEqualTo("ins_user");
        assertThat(func.apply("ins")).isEqualTo("ins");
        assertThat(func.apply("123")).isEqualTo("123");
        assertThat(func.apply("123-fab")).isNull();

        func = ByTablePatternsTopicNamingStrategy.mapRegex("(\\w+)(\\.(\\w+))+ = $1");
        assertThat(func.apply("abc.def")).isEqualTo("abc");
        assertThat(func.apply("a.d")).isEqualTo("a");
        assertThat(func.apply("a.d.c.e")).isEqualTo("a");
    }
    
    @Test
    public void shouldComputeTopicNameFromJustPrefixAndDatabaseUsingPatterns() {
        // mandatory table name in fqn ...
        s = new ByTablePatternsTopicNamingStrategy("(\\w+)(\\.(\\w+))+ = $1");
        assertThat(s.getTopic("abc", "db1", "t1")).isEqualTo("abc.db1");
        assertThat(s.getTopic("abc", "db1", "")).isNull(); // no match
        assertThat(s.getTopic("abc", "db1", null)).isNull(); // no match

        // optional table name in fqn ...
        s = new ByTablePatternsTopicNamingStrategy("(\\w+)(\\.(\\w+))? = $1");
        assertThat(s.getTopic("abc", "db1", "t1")).isEqualTo("abc.db1");
        assertThat(s.getTopic("abc", "db1", "")).isEqualTo("abc.db1");
        assertThat(s.getTopic("abc", "db1", null)).isEqualTo("abc.db1");
    }
}
