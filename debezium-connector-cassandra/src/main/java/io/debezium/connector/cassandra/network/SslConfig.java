/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.network;

import java.util.Map;

public class SslConfig {

    public static final String KEY_STORE_LOCATION = "keyStore.location";
    public static final String KEY_STORE_PASSWORD = "keyStore.password";
    public static final String KEY_STORE_TYPE = "keyStore.type";
    public static final String DEFAULT_KEY_STORE_TYPE = "JKS";

    public static final String TRUST_STORE_LOCATION = "trustStore.location";
    public static final String TRUST_STORE_PASSWORD = "trustStore.password";
    public static final String TRUST_STORE_TYPE = "trustStore.type";
    public static final String DEFAULT_TRUST_STORE_TYPE = "JKS";

    public static final String KEY_MANAGER_ALGORITHM = "keyManager.algorithm";
    public static final String DEFAULT_KEY_MANAGER_ALGORITHM = "SunX509";

    public static final String TRUST_MANAGER_ALGORITHM = "trustManager.algorithm";
    public static final String DEFAULT_TRUST_MANAGER_ALGORITHM = "SunX509";

    private Map<String, Object> configs;

    public SslConfig(Map<String, Object> configs) {
        this.configs = configs;
    }

    public String keyStoreLocation() {
        return (String) configs.get(KEY_STORE_LOCATION);
    }

    public String keyStorePassword() {
        return (String) configs.get(KEY_STORE_PASSWORD);
    }

    public String keyStoreType() {
        return (String) configs.getOrDefault(KEY_STORE_TYPE, DEFAULT_KEY_STORE_TYPE);
    }

    public String getKeyManagerAlgorithm() {
        return (String) configs.getOrDefault(KEY_MANAGER_ALGORITHM, DEFAULT_KEY_MANAGER_ALGORITHM);
    }

    public String trustStoreLocation() {
        return (String) configs.get(TRUST_STORE_LOCATION);
    }

    public String trustStorePassword() {
        return (String) configs.get(TRUST_STORE_PASSWORD);
    }

    public String trustStoreType() {
        return (String) configs.getOrDefault(TRUST_STORE_TYPE, DEFAULT_TRUST_STORE_TYPE);
    }

    public String trustManagerAlgorithm() {
        return (String) configs.getOrDefault(TRUST_MANAGER_ALGORITHM, DEFAULT_TRUST_MANAGER_ALGORITHM);
    }
}
