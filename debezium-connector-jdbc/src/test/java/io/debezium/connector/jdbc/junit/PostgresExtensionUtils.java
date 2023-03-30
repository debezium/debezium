/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit;

import io.debezium.connector.jdbc.junit.jupiter.Sink;
import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.util.Strings;

/**
 * Utility class to handle postgres extension creation and removal.
 *
 * @author Chris Cranford
 */
public class PostgresExtensionUtils {

    public static final String EXTENSION_POSTGIS = "postgis";

    public static void createExtension(Source source, String extensionName) throws Exception {
        if (!Strings.isNullOrBlank(extensionName)) {
            System.out.println("Creating source extension " + extensionName);
            if (EXTENSION_POSTGIS.equalsIgnoreCase(extensionName)) {
                source.execute("DROP EXTENSION IF EXISTS postgis CASCADE");
                source.execute("DROP SCHEMA IF EXISTS postgis CASCADE");
                source.execute("CREATE SCHEMA postgis");
                source.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA postgis");
            }
            else {
                source.execute("CREATE EXTENSION " + extensionName);
            }
            source.close();
        }
    }

    public static void dropExtension(Source source, String extensionName) throws Exception {
        if (!Strings.isNullOrEmpty(extensionName)) {
            System.out.println("Dropping source extension " + extensionName);
            if (EXTENSION_POSTGIS.equalsIgnoreCase(extensionName)) {
                source.execute("DROP EXTENSION postgis CASCADE");
                source.execute("DROP SCHEMA IF EXISTS postgis CASCADE");
            }
            else {
                source.execute("DROP EXTENSION " + extensionName + " CASCADE");
            }
        }
    }

    public static void createExtension(Sink sink, String extensionName) throws Exception {
        if (!Strings.isNullOrBlank(extensionName)) {
            System.out.println("Creating sink extension " + extensionName);
            if (EXTENSION_POSTGIS.equalsIgnoreCase(extensionName)) {
                sink.execute("DROP EXTENSION IF EXISTS postgis CASCADE");
                sink.execute("DROP SCHEMA IF EXISTS postgis CASCADE");
                sink.execute("CREATE SCHEMA postgis");
                sink.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA postgis");
                sink.execute("CREATE TYPE geometry AS (udt postgis.geometry)");
            }
            else {
                sink.execute("CREATE EXTENSION " + extensionName);
            }
            sink.close();
        }
    }

    public static void dropExtension(Sink sink, String extensionName) throws Exception {
        if (!Strings.isNullOrEmpty(extensionName)) {
            System.out.println("Dropping sink extension " + extensionName);
            if (EXTENSION_POSTGIS.equalsIgnoreCase(extensionName)) {
                sink.execute("DROP EXTENSION postgis CASCADE");
                sink.execute("DROP SCHEMA IF EXISTS postgis CASCADE");
            }
            else {
                sink.execute("DROP EXTENSION " + extensionName + " CASCADE");
            }
        }
    }
}
