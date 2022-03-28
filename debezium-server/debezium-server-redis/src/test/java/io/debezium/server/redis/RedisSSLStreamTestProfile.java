/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.redis;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;

public class RedisSSLStreamTestProfile implements QuarkusTestProfile {

    @Override
    public List<TestResourceEntry> testResources() {
        return Arrays.asList(
                new TestResourceEntry(PostgresTestResourceLifecycleManager.class),
                new TestResourceEntry(RedisSSLTestResourceLifecycleManager.class));
    }

    public Map<String, String> getConfigOverrides() {

        Map<String, String> config = new HashMap<String, String>();
        URL keyStoreFile = RedisSSLStreamTestProfile.class.getClassLoader().getResource("ssl/client-keystore.p12");
        URL trustStoreFile = RedisSSLStreamTestProfile.class.getClassLoader().getResource("ssl/client-truststore.p12");

        config.put("javax.net.ssl.keyStore", keyStoreFile.getPath());
        config.put("javax.net.ssl.trustStore", trustStoreFile.getPath());
        config.put("javax.net.ssl.keyStorePassword", "secret");
        config.put("javax.net.ssl.trustStorePassword", "secret");
        config.put("debezium.source.offset.storage", "io.debezium.server.redis.RedisOffsetBackingStore");
        config.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        return config;
    }

}
