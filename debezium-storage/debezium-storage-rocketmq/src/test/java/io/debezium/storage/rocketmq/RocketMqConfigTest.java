/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.rocketmq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class RocketMqConfigTest {

    private static final String SECRET_KEY = "super-secret-key-value";

    @Test
    public void toStringShouldMaskSecretKey() {
        RocketMqConfig config = RocketMqConfig.newBuilder()
                .namesrvAddr("localhost:9876")
                .groupId("group")
                .aclEnable(true)
                .accessKey("access-key")
                .secretKey(SECRET_KEY)
                .build();

        String rendered = config.toString();

        assertFalse(rendered.contains(SECRET_KEY), "toString() must not expose the raw secret key");
        assertTrue(rendered.contains("secretKey='********'"), "toString() should mask the secret key");
        // Non-sensitive fields must remain visible for troubleshooting.
        assertTrue(rendered.contains("namesrvAddr='localhost:9876'"));
        assertTrue(rendered.contains("accessKey='access-key'"));
    }

    @Test
    public void toStringShouldNotMaskNullSecretKey() {
        RocketMqConfig config = RocketMqConfig.newBuilder()
                .namesrvAddr("localhost:9876")
                .groupId("group")
                .build();

        assertTrue(config.toString().contains("secretKey='null'"));
    }
}
