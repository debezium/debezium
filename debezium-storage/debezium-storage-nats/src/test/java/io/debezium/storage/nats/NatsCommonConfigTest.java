/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.nats;

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(natsConfig.getUser()).isEqualTo("debezium");
        assertThat(natsConfig.getPassword()).isEqualTo("secret");
        assertThat(natsConfig.getToken()).containsExactly("tokensecret".toCharArray());
        assertThat(natsConfig.isTlsEnabled()).isTrue();
        assertThat(natsConfig.getTlsTruststorePath()).isEqualTo("/tmp/truststore.jks");
        assertThat(natsConfig.getTlsTruststorePassword()).isEqualTo("changeit");
        assertThat(natsConfig.getTlsTruststoreType()).isEqualTo("PKCS12");
        assertThat(natsConfig.getTlsKeystorePath()).isEqualTo("/tmp/keystore.jks");
        assertThat(natsConfig.getTlsKeystorePassword()).isEqualTo("changeit");
        assertThat(natsConfig.getTlsKeystoreType()).isEqualTo("PKCS12");
    }

    @Test
    public void shouldDefaultTlsDisabledAndEmptyCredentials() {
        Map<String, String> props = new HashMap<>();
        props.put("nats.url", "nats://localhost:4222");
        Configuration config = Configuration.from(props);

        NatsCommonConfig natsConfig = new NatsCommonConfig(config);

        assertThat(natsConfig.isTlsEnabled()).isFalse();
        assertThat(natsConfig.getUser()).isEmpty();
        assertThat(natsConfig.getPassword()).isEmpty();
        assertThat(natsConfig.getToken()).isEmpty();
        assertThat(natsConfig.getTlsTruststoreType()).isEqualTo("JKS");
        assertThat(natsConfig.getTlsKeystoreType()).isEqualTo("JKS");
    }
}
