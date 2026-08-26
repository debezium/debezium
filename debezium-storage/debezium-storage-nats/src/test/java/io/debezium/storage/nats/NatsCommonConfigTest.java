/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link NatsCommonConfig} parsing, including auth and TLS
 * properties. No NATS server is required.
 *
 * @author Nick Chomey
 */
class NatsCommonConfigTest {

    @Test
    public void shouldParseAuthAndTlsConfiguration() {
        Map<String, String> props = new HashMap<>();
        props.put("nats.url", "nats://localhost:4222");
        props.put("nats.user", "debezium");
        props.put("nats.password", "secret");
        props.put("nats.token", "tokensecret");
        props.put("nats.tls.enabled", "true");
        props.put("nats.tls.truststore.path", "/tmp/truststore.jks");
        props.put("nats.tls.truststore.password", "changeit");
        props.put("nats.tls.truststore.type", "PKCS12");
        props.put("nats.tls.keystore.path", "/tmp/keystore.jks");
        props.put("nats.tls.keystore.password", "changeit");
        props.put("nats.tls.keystore.type", "PKCS12");
        Configuration config = Configuration.from(props);

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        assertEquals("debezium", natsConfig.getUser());
        assertEquals("secret", natsConfig.getPassword());
        assertEquals("tokensecret", natsConfig.getToken());
        assertTrue(natsConfig.isTlsEnabled());
        assertEquals("/tmp/truststore.jks", natsConfig.getTlsTruststorePath());
        assertEquals("changeit", natsConfig.getTlsTruststorePassword());
        assertEquals("PKCS12", natsConfig.getTlsTruststoreType());
        assertEquals("/tmp/keystore.jks", natsConfig.getTlsKeystorePath());
        assertEquals("changeit", natsConfig.getTlsKeystorePassword());
        assertEquals("PKCS12", natsConfig.getTlsKeystoreType());
    }

    @Test
    public void shouldDefaultTlsDisabledAndEmptyCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("nats.url", "nats://localhost:4222");
        Configuration config = Configuration.from(props);

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        assertFalse(natsConfig.isTlsEnabled());
        assertEquals("", natsConfig.getUser());
        assertEquals("", natsConfig.getPassword());
        assertEquals("", natsConfig.getToken());
        assertEquals("JKS", natsConfig.getTlsTruststoreType());
        assertEquals("JKS", natsConfig.getTlsKeystoreType());
    }
}