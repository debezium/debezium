/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class OraclePartitionTest {

    @Test
    public void equalPartitionsShouldBeEqual() {
        assertThat(new OraclePartition()).isEqualTo(new OraclePartition());
    }

    @Test
    public void equalPartitionsShouldHaveEqualHashCodes() {
        assertThat(new OraclePartition().hashCode()).isEqualTo(new OraclePartition().hashCode());
    }
}
