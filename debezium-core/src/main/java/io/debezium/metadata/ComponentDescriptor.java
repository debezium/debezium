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

    private static final String SINK_CONNECTOR_TYPE = "sink-connector";
    private static final String SOURCE_CONNECTOR_TYPE = "source-connector";
    private static final String TRANSFORMATION_TYPE = "transformation";
    private static final String PREDICATE_TYPE = "predicate";
    private static final String CONVERTER_TYPE = "converter";
    private static final String CUSTOM_CONVERTER_TYPE = "custom-converter";
    private static final String UNKNOWN_TYPE = "unknown";

    /**
     * Mapping of Kafka Connect interface names to component type identifiers.
     * Checked in order, so more specific types should come first.
     */
    private static final Map<String, String> COMPONENT_TYPE_MAPPINGS = Map.of(
            "org.apache.kafka.connect.source.SourceConnector", SOURCE_CONNECTOR_TYPE,
            "org.apache.kafka.connect.sink.SinkConnector", SINK_CONNECTOR_TYPE,
            "org.apache.kafka.connect.transforms.Transformation", TRANSFORMATION_TYPE,
            "org.apache.kafka.connect.transforms.predicates.Predicate", PREDICATE_TYPE,
            "org.apache.kafka.connect.storage.Converter", CONVERTER_TYPE,
            "io.debezium.spi.converter.CustomConverter", CUSTOM_CONVERTER_TYPE);

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
        this(className, getDisplayNameForConnectorClass(className), className, version,
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
                        return UNKNOWN_TYPE;
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
            return SINK_CONNECTOR_TYPE;
        }
        else if (className.contains("SourceConnector") || className.contains("Source")) {
            return SOURCE_CONNECTOR_TYPE;
        }
        else if (className.endsWith("Connector")) {
            // Debezium convention: XyzConnector (without Sink prefix) is a source connector
            return SOURCE_CONNECTOR_TYPE;
        }
        else if (className.contains("Transformation") || className.contains("Transform")) {
            return TRANSFORMATION_TYPE;
        }
        else if (className.contains("Predicate")) {
            return PREDICATE_TYPE;
        }
        return UNKNOWN_TYPE;
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
