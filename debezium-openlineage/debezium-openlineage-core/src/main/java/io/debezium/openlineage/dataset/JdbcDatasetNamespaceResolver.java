/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.dataset;

import static io.debezium.openlineage.dataset.DefaultDatabaseNamespaceResolver.INPUT_DATASET_NAMESPACE_FORMAT;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcDatasetNamespaceResolver implements DatasetNamespaceResolver {

    private static final String CONNECTION_URL = "connection.url";
    private static final Pattern JDBC_PATTERN = Pattern.compile("^jdbc:([^:]+)(?::thin:@(?://)?|://)([^:/]+)(?::(\\d+))?");
    private static final Map<String, String> DEFAULT_PORTS = Map.of(
            "mysql", "3306",
            "mariadb", "3306",
            "postgresql", "5432",
            "oracle", "1521",
            "sqlserver", "1433",
            "mongodb", "27017",
            "db2", "50000");

    @Override
    public String resolve(Map<String, String> configuration, String connectorName) {

        String connectionUrl = configuration.get(CONNECTION_URL);

        if (connectionUrl == null) {
            return "unknown:unknown";
        }

        Matcher matcher = JDBC_PATTERN.matcher(connectionUrl);
        if (!matcher.find()) {
            return "unknown:unknown";
        }

        String database = matcher.group(1);
        String hostname = matcher.group(2);
        String port = matcher.group(3) != null ? matcher.group(3) : DEFAULT_PORTS.getOrDefault(database.toLowerCase(), "");

        if ("postgresql".equalsIgnoreCase(database)) {
            database = "postgres"; // Required to match OpenLineage naming convention https://openlineage.io/docs/spec/naming
        }

        return String.format(INPUT_DATASET_NAMESPACE_FORMAT, database, hostname, port);
    }

}
