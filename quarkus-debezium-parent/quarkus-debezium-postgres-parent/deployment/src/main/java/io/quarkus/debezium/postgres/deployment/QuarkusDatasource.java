/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.postgres.deployment;

import java.net.URI;
import java.util.Map;

public class QuarkusDatasource {
    public static Map<String, String> generateDebeziumConfiguration(Map<String, String> properties) {
        URI uri = URI.create(properties.get("quarkus.datasource.jdbc.url").substring(5));

        return Map.of(
                "quarkus.debezium.database.hostname", uri.getHost(),
                "quarkus.debezium.database.user", properties.get("quarkus.datasource.username"),
                "quarkus.debezium.database.password", properties.get("quarkus.datasource.password"),
                "quarkus.debezium.database.dbname", uri.getPath().substring(1),
                "quarkus.debezium.database.port", String.valueOf(uri.getPort()));
    }
}