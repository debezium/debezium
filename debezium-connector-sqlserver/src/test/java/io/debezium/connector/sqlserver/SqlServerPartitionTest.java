/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class SqlServerPartitionTest {

    @Test
    public void equalPartitionsShouldBeEqual() {
        assertThat(new SqlServerPartition("database1", false))
                .isEqualTo(new SqlServerPartition("database1", false));
    }

    @Test
    public void nonEqualPartitionsShouldNotBeEqual() {
        assertThat(new SqlServerPartition("database1", false))
                .isNotEqualTo(new SqlServerPartition("database2", false));
    }

    @Test
    public void equalPartitionsShouldHaveEqualHashCodes() {
        assertThat(new SqlServerPartition("database1", false).hashCode())
                .isEqualTo(new SqlServerPartition("database1", false).hashCode());
    }

    @Test
    public void nonEqualPartitionsShouldHaveNonEqualHashCodes() {
        assertThat(new SqlServerPartition("database1", false).hashCode())
                .isNotEqualTo(new SqlServerPartition("database2", false).hashCode());
    }
}
