/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.configmap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.Test;

public class ConfigMapFormatterTest {

    final ConfigMapFormatter formatter = new ConfigMapFormatter();

    @Test
    public void convertFromStorableFormat() {

        Map<ByteBuffer, ByteBuffer> originalFormat = formatter.convertFromStorableFormat(Map.of(
                "kafka.server-inventory_database-db1",
                "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6dHJ1ZSwibHNuIjozNzkyOTM3NiwidHhJZCI6Nzc1LCJ0c191c2VjIjoxNzMwMTEwNjEzNzY2Njc0LCJzbmFwc2hvdCI6dHJ1ZX0="));

        assertThat(originalFormat).containsExactly(
                entry(ByteBuffer.wrap("[\"kafka\",{\"server\":\"inventory\",\"database\":\"db1\"}]".getBytes(StandardCharsets.UTF_8)),
                        ByteBuffer.wrap(("{\"last_snapshot_record\":true,\"lsn\":37929376,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":true}".getBytes(
                                StandardCharsets.UTF_8)))));
    }

    @Test
    public void convertToStorableFormat() {

        Map<String, String> storableFormat = formatter.convertToStorableFormat(Map.of(
                ByteBuffer.wrap("[\"kafka\",{\"server\":\"inventory\",\"database\":\"db1\"}]".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(("{\"last_snapshot_record\":true,\"lsn\":37929376,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":true}".getBytes(
                        StandardCharsets.UTF_8)))));

        assertThat(storableFormat).containsExactly(
                entry("kafka.server-inventory_database-db1",
                        "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6dHJ1ZSwibHNuIjozNzkyOTM3NiwidHhJZCI6Nzc1LCJ0c191c2VjIjoxNzMwMTEwNjEzNzY2Njc0LCJzbmFwc2hvdCI6dHJ1ZX0="));
    }
}
