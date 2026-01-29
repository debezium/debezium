/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentDescriptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentDescriptor.class);

    /**
     * Mapping of Kafka Connect interface names to component type identifiers.
     * Checked in order, so more specific types should come first.
     */
    private static final Map<String, String> COMPONENT_TYPE_MAPPINGS = Map.of(
            "org.apache.kafka.connect.source.SourceConnector", "source-connector",
            "org.apache.kafka.connect.sink.SinkConnector", "sink-connector",
            "org.apache.kafka.connect.transforms.Transformation", "transformation",
            "org.apache.kafka.connect.transforms.predicates.Predicate", "predicate");

    private final String id;
    private final String displayName;
    private final String className;
    private final String version;
    private final String type;

    private ComponentDescriptor(String id, String displayName, String className, String version, String type) {
        this.id = id;
        this.displayName = displayName;
        this.className = className;
        this.version = version;
        this.type = type;
    }

    public ComponentDescriptor(String className, String version) {
        this(getIdForConnectorClass(className), getDisplayNameForConnectorClass(className), className, version,
                determineComponentType(className));
    }

    public String getId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getClassName() {
        return className;
    }

    public String getVersion() {
        return version;
    }

    public String getType() {
        return type;
    }

    /**
     * Determines the component type by checking which Kafka Connect interface the class implements.
     * This is more reliable than string matching on class names.
     *
     * @param className the fully qualified class name
     * @return the component type: "source-connector", "sink-connector", "transformation", "predicate", or "unknown"
     */
    private static String determineComponentType(String className) {
        try {
            Class<?> componentClass = Class.forName(className);

            return COMPONENT_TYPE_MAPPINGS.entrySet().stream()
                    .filter(entry -> isAssignableFrom(componentClass, entry.getKey()))
                    .map(Map.Entry::getValue)
                    .findFirst()
                    .orElseGet(() -> {
                        LOGGER.warn("Component class {} does not implement any recognized Kafka Connect interface", className);
                        return "unknown";
                    });
        }
        catch (ClassNotFoundException e) {
            LOGGER.warn("Could not load component class {}, falling back to name-based detection", className);
            return determineComponentTypeByName(className);
        }
    }

    /**
     * Fallback method that determines component type based on class name patterns.
     * Used when the class cannot be loaded via reflection.
     * <p>
     * Debezium naming convention: Classes ending in "Connector" (without "Sink" prefix)
     * are source connectors by default.
     */
    private static String determineComponentTypeByName(String className) {
        if (className.contains("SinkConnector") || className.contains("Sink")) {
            return "sink-connector";
        }
        else if (className.contains("SourceConnector") || className.contains("Source")) {
            return "source-connector";
        }
        else if (className.endsWith("Connector")) {
            // Debezium convention: XyzConnector (without Sink prefix) is a source connector
            return "source-connector";
        }
        else if (className.contains("Transformation") || className.contains("Transform")) {
            return "transformation";
        }
        else if (className.contains("Predicate")) {
            return "predicate";
        }
        return "unknown";
    }

    /**
     * Checks if the given class is assignable from the specified interface/class name.
     * Returns false if the interface class cannot be loaded.
     */
    private static boolean isAssignableFrom(Class<?> componentClass, String interfaceClassName) {
        try {
            Class<?> interfaceClass = Class.forName(interfaceClassName);
            return interfaceClass.isAssignableFrom(componentClass);
        }
        catch (ClassNotFoundException e) {
            LOGGER.debug("Could not load interface class {}", interfaceClassName);
            return false;
        }
    }

    public static String getIdForConnectorClass(String className) {
        return switch (className) {
            case "io.debezium.connector.mongodb.MongoDbConnector" -> "mongodb";
            case "io.debezium.connector.mysql.MySqlConnector" -> "mysql";
            case "io.debezium.connector.oracle.OracleConnector" -> "oracle";
            case "io.debezium.connector.postgresql.PostgresConnector" -> "postgres";
            case "io.debezium.connector.sqlserver.SqlServerConnector" -> "sqlserver";
            case "io.debezium.connector.mariadb.MariaDbConnector" -> "mariadb";
            default -> className;
        };
    }

    public static String getDisplayNameForConnectorClass(String className) {
        return switch (className) {
            case "io.debezium.connector.mongodb.MongoDbConnector" -> "Debezium MongoDB Connector";
            case "io.debezium.connector.mysql.MySqlConnector" -> "Debezium MySQL Connector";
            case "io.debezium.connector.oracle.OracleConnector" -> "Debezium Oracle Connector";
            case "io.debezium.connector.postgresql.PostgresConnector" -> "Debezium PostgreSQL Connector";
            case "io.debezium.connector.sqlserver.SqlServerConnector" -> "Debezium SQLServer Connector";
            case "io.debezium.connector.mariadb.MariaDbConnector" -> "Debezium MariaDB Connector";
            default -> className;
        };
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null || getClass() != that.getClass()) {
            return false;
        }
        return this.getClassName().equals(((ComponentDescriptor) that).getClassName())
                && this.getVersion().equals(((ComponentDescriptor) that).getVersion());
    }

    public int hashCode() {
        return Objects.hash(this.className, this.version);
    }
}
