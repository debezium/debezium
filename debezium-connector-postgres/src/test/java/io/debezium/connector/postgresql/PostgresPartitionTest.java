/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class PostgresPartitionTest {

    @Test
    public void equalPartitionsShouldBeEqual() {
        assertThat(new PostgresPartition()).isEqualTo(new PostgresPartition());
    }

    @Test
    public void equalPartitionsShouldHaveEqualHashCodes() {
        assertThat(new PostgresPartition().hashCode()).isEqualTo(new PostgresPartition().hashCode());
    }
}
