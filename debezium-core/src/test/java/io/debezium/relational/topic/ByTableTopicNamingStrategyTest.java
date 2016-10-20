/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.topic;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class ByTableTopicNamingStrategyTest {

    private final ByTableTopicNamingStrategy strategy = new ByTableTopicNamingStrategy();
    
    @Test
    public void shouldReturnPrefixWhenDatabaseIsNullOrEmpty() {
        assertThat(strategy.getTopic("abc", null, null)).isEqualTo("abc");
        assertThat(strategy.getTopic("abc", "", null)).isEqualTo("abc");
    }

    @Test
    public void shouldExcludeNullOrEmptyTableName() {
        assertThat(strategy.getTopic("abc", "db", null)).isEqualTo("abc.db");
        assertThat(strategy.getTopic("abc", "db", "")).isEqualTo("abc.db");
    }

    @Test
    public void shouldReturnTopicNames() {
        assertThat(strategy.getTopic("abc", "db", "t1")).isEqualTo("abc.db.t1");
        assertThat(strategy.getTopic("abc.b.c", "db", "t1")).isEqualTo("abc.b.c.db.t1");
    }

}
