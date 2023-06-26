/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.ConnectionString;

import io.debezium.DebeziumException;
import io.debezium.util.Strings;

/**
 * Host string parsing utilities
 */
public final class ConnectionStrings {

    private ConnectionStrings() {
        // intentionally private;
    }

    /**
     * Regular expression that extracts the hosts for the replica sets. The raw expression is
     * {@code (([^/]+)\/))?(.+)}.
     */
    private static final Pattern HOST_PATTERN = Pattern.compile("(([^/]+)\\/)?(.+)");

    public static Optional<String> parseFromHosts(String hosts) {
        return matcher(hosts).map(m -> connectionString(m.group(2), m.group(3)));
    }

    public static String buildFromHosts(String hosts) {
        return parseFromHosts(hosts).orElseThrow(() -> new DebeziumException("Unable to build connection string"));
    }

    /**
     * Appends new parameter to connection string
     *
     * @param connectionString original connection string
     * @param name parameter name
     * @param value parameter value
     * @return new connection string with added parameter
     */
    public static String appendParameter(String connectionString, String name, String value) {
        var param = name + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8);

        if (connectionString.endsWith("?")) {
            return connectionString + param;
        }
        if (connectionString.endsWith("/")) {
            return connectionString + "?" + param;
        }

        var pos = connectionString.lastIndexOf("?");
        if (pos == -1) {
            return connectionString + "/?" + param;
        }

        return connectionString + "&" + param;
    }

    public static String appendParameters(String connectionString, String parameters) {
        if (parameters == null || parameters.isBlank()) {
            return connectionString;
        }
        if (connectionString.endsWith("?")) {
            return connectionString + parameters;
        }
        if (connectionString.endsWith("/")) {
            return connectionString + "?" + parameters;
        }

        var pos = connectionString.lastIndexOf("?");
        if (pos == -1) {
            return connectionString + "/?" + parameters;
        }

        return connectionString + "&" + parameters;
    }

    /**
     * Mask credential information in connection string
     *
     * @param connectionString original connection string
     * @return connection string with masked credential information
     */
    public static String mask(String connectionString) {
        var cs = new ConnectionString(connectionString);
        var credentials = cs.getCredential();

        return credentials == null ? connectionString
                : Strings.mask(
                        connectionString,
                        credentials.getUserName(),
                        credentials.getSource(),
                        credentials.getPassword() != null ? String.valueOf(credentials.getPassword()) : null);
    }

    private static String connectionString(String rsName, String host) {
        if (rsName == null) {
            return String.format("mongodb://%s/", host);
        }
        else {
            return String.format("mongodb://%s/?replicaSet=%s", host, rsName);
        }
    }

    private static Optional<Matcher> matcher(String hosts) {
        if (hosts == null || hosts.isBlank()) {
            return Optional.empty();
        }
        Matcher matcher = HOST_PATTERN.matcher(hosts);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return Optional.of(matcher);
    }
}
