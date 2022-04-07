/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

public class ReplicaSetPartitionTest {

    @Test
    public void equalPartitionsShouldBeEqual() {
        assertThat(new ReplicaSetPartition("server1", "rs1"))
                .isEqualTo(new ReplicaSetPartition("server1", "rs1"));
    }

    @Test
    public void nonEqualPartitionsShouldNotBeEqual() {
        assertThat(new ReplicaSetPartition("server1", "rs1"))
                .isNotEqualTo(new ReplicaSetPartition("server1", "rs2"));
        assertThat(new ReplicaSetPartition("server1", "rs1"))
                .isNotEqualTo(new ReplicaSetPartition("server2", "rs1"));
    }

    @Test
    public void equalPartitionsShouldHaveEqualHashCodes() {
        assertThat(new ReplicaSetPartition("server1", "rs1").hashCode())
                .isEqualTo(new ReplicaSetPartition("server1", "rs1").hashCode());
    }

    @Test
    public void nonEqualPartitionsShouldHaveNonEqualHashCodes() {
        assertThat(new ReplicaSetPartition("server1", "rs1").hashCode())
                .isNotEqualTo(new ReplicaSetPartition("server1", "rs2").hashCode());
        assertThat(new ReplicaSetPartition("server1", "rs1").hashCode())
                .isNotEqualTo(new ReplicaSetPartition("server2", "rs1").hashCode());
    }
}
