/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class DefaultTransactionInfoTest {

    @Test
    public void testGetId() {
        String expectedId = "id";
        DefaultTransactionInfo info = new DefaultTransactionInfo(expectedId);
        assertThat(info.getTransactionId()).isEqualTo(expectedId);
    }

}
