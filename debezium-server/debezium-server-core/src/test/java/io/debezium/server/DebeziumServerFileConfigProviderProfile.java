/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class DebeziumServerFileConfigProviderProfile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
        Map<String, String> config = new HashMap<String, String>();
        URL secretFile = DebeziumServerFileConfigProviderProfile.class.getClassLoader().getResource("secrets_test.txt");

        config.put("debezium.source.database.user", "\\${file:" + secretFile.getPath() + ":user}");

        config.put("debezium.source.config.providers", "file");
        config.put("debezium.source.config.providers.file.class", "org.apache.kafka.common.config.provider.FileConfigProvider");

        return config;
    }

}
