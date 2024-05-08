/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.pipeline.spi.Partition;

abstract public class AbstractPartitionTest<P extends Partition> {

    @Test
    public void equalPartitionsShouldBeEqual() {
        assertThat(createPartition1()).isEqualTo(createPartition1());
    }

    @Test
    public void nonEqualPartitionsShouldNotBeEqual() {
        assertThat(createPartition1()).isNotEqualTo(createPartition2());
    }

    @Test
    public void equalPartitionsShouldHaveEqualHashCodes() {
        assertThat(createPartition1().hashCode()).isEqualTo(createPartition1().hashCode());
    }

    @Test
    public void nonEqualPartitionsShouldHaveNonEqualHashCodes() {
        assertThat(createPartition1().hashCode()).isNotEqualTo(createPartition2().hashCode());
    }

    protected abstract P createPartition1();

    protected abstract P createPartition2();
}
