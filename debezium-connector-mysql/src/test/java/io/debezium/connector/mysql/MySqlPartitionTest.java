/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class MySqlPartitionTest {

    @Test
    public void equalPartitionsShouldBeEqual() {
        assertThat(new MySqlPartition()).isEqualTo(new MySqlPartition());
    }

    @Test
    public void equalPartitionsShouldHaveEqualHashCodes() {
        assertThat(new MySqlPartition().hashCode()).isEqualTo(new MySqlPartition().hashCode());
    }
}
