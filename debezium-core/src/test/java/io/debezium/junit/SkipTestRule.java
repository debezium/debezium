/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reflections.Reflections;

import io.debezium.junit.DatabaseVersionResolver.DatabaseVersion;
import io.debezium.util.JvmVersionUtil;
import io.debezium.util.Testing;

/**
 * JUnit rule that inspects the presence of the {@link SkipLongRunning} annotation either on a test method or on a test suite. If
 * it finds the annotation, it will only run the test method/suite if the system property {@code skipLongRunningTests} has the
 * value {@code true}
 *
 * @author Horia Chiorean
 */
public class SkipTestRule extends AnnotationBasedTestRule {

    @Override
    public Statement apply(Statement base,
                           Description description) {
        SkipWhenJavaVersion skipJavaVersionAnnotation = hasAnnotation(description, SkipWhenJavaVersion.class);
        if (skipJavaVersionAnnotation != null) {
            int checkedVersion = skipJavaVersionAnnotation.value();
            int actualVersion = JvmVersionUtil.getFeatureVersion();
            boolean isSkippedVersion;

            switch (skipJavaVersionAnnotation.check()) {
                case EQUAL:
                    isSkippedVersion = actualVersion == checkedVersion;
                case GREATER_THAN:
                    isSkippedVersion = actualVersion > checkedVersion;
                    break;
                case GREATER_THAN_OR_EQUAL:
                    isSkippedVersion = actualVersion >= checkedVersion;
                    break;
                case LESS_THAN:
                    isSkippedVersion = actualVersion < checkedVersion;
                    break;
                case LESS_THAN_OR_EQUAL:
                    isSkippedVersion = actualVersion <= checkedVersion;
                    break;
                default:
                    isSkippedVersion = false;
                    break;
            }

            if (isSkippedVersion) {
                return emptyStatement("Java version=" + actualVersion, description);
            }
        }

        SkipLongRunning skipLongRunningAnnotation = hasAnnotation(description, SkipLongRunning.class);
        if (skipLongRunningAnnotation != null) {
            String skipLongRunning = System.getProperty(SkipLongRunning.SKIP_LONG_RUNNING_PROPERTY);
            if (skipLongRunning == null || Boolean.valueOf(skipLongRunning)) {
                return emptyStatement(skipLongRunningAnnotation.value(), description);
            }
        }

        SkipOnOS skipOnOSAnnotation = hasAnnotation(description, SkipOnOS.class);
        if (skipOnOSAnnotation != null) {
            String[] oses = skipOnOSAnnotation.value();
            String osName = System.getProperty("os.name");
            if (osName != null && !osName.trim().isEmpty()) {
                for (String os : oses) {
                    if (osName.toLowerCase().startsWith(os.toLowerCase())) {
                        return emptyStatement(skipOnOSAnnotation.description(), description);
                    }
                }
            }
        }

        SkipWhenKafkaVersion skipWhenKafkaVersionAnnotation = hasAnnotation(description, SkipWhenKafkaVersion.class);
        if (skipWhenKafkaVersionAnnotation != null) {
            SkipWhenKafkaVersion.KafkaVersion kafkaVersion = skipWhenKafkaVersionAnnotation.value();
            EqualityCheck check = skipWhenKafkaVersionAnnotation.check();
            try (InputStream stream = Testing.class.getResourceAsStream("/kafka/kafka-version.properties")) {
                if (stream != null) {
                    final Properties properties = new Properties();
                    properties.load(stream);

                    final String version = properties.getProperty("version");
                    final String[] versionArray = version.split("\\.");

                    int major = (versionArray.length >= 1) ? Integer.parseInt(versionArray[0]) : 0;
                    int minor = (versionArray.length >= 2) ? Integer.parseInt(versionArray[1]) : 1;
                    int patch = (versionArray.length >= 3) ? Integer.parseInt(versionArray[2]) : 2;

                    switch (check) {
                        case LESS_THAN:
                            if (kafkaVersion.isLessThan(major, minor, patch)) {
                                return emptyStatement(skipWhenKafkaVersionAnnotation.description(), description);
                            }
                            break;
                        case LESS_THAN_OR_EQUAL:
                            if (kafkaVersion.isLessThanOrEqualTo(major, minor, patch)) {
                                return emptyStatement(skipWhenKafkaVersionAnnotation.description(), description);
                            }
                            break;
                        case EQUAL:
                            if (kafkaVersion.isEqualTo(major, minor, patch)) {
                                return emptyStatement(skipWhenKafkaVersionAnnotation.description(), description);
                            }
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            if (kafkaVersion.isGreaterThanOrEqualTo(major, minor, patch)) {
                                return emptyStatement(skipWhenKafkaVersionAnnotation.description(), description);
                            }
                            break;
                        case GREATER_THAN:
                            if (kafkaVersion.isGreaterThan(major, minor, patch)) {
                                return emptyStatement(skipWhenKafkaVersionAnnotation.description(), description);
                            }
                            break;
                    }
                }
            }
            catch (IOException e) {
                // In the event of an error determining the kafka version, run test.
            }
        }

        final SkipWhenConnectorUnderTest connectorUnderTestAnnotation = hasAnnotation(description, SkipWhenConnectorUnderTest.class);
        if (connectorUnderTestAnnotation != null) {
            if (isSkippedByConnectorUnderTest(description, connectorUnderTestAnnotation)) {
                return emptyStatement("Connector under test " + connectorUnderTestAnnotation.value(), description);
            }
        }

        final SkipWhenConnectorsUnderTest connectorsUnderTestAnnotation = hasAnnotation(description, SkipWhenConnectorsUnderTest.class);
        if (connectorsUnderTestAnnotation != null) {
            for (SkipWhenConnectorUnderTest connector : connectorsUnderTestAnnotation.value()) {
                if (isSkippedByConnectorUnderTest(description, connector)) {
                    return emptyStatement("Connector under test " + connector.value(), description);
                }
            }
        }

        // First check if multiple database version skips are specified.
        SkipWhenDatabaseVersions skipWhenDatabaseVersions = hasAnnotation(description, SkipWhenDatabaseVersions.class);
        if (skipWhenDatabaseVersions != null) {
            for (SkipWhenDatabaseVersion skipWhenDatabaseVersion : skipWhenDatabaseVersions.value()) {
                if (isSkippedByDatabaseVersion(skipWhenDatabaseVersion)) {
                    return emptyStatement(skipWhenDatabaseVersion.reason(), description);
                }
            }
        }

        // Now check if a single database version skip is specified.
        SkipWhenDatabaseVersion skipWhenDatabaseVersion = hasAnnotation(description, SkipWhenDatabaseVersion.class);
        if (skipWhenDatabaseVersion != null) {
            if (isSkippedByDatabaseVersion(skipWhenDatabaseVersion)) {
                return emptyStatement(skipWhenDatabaseVersion.reason(), description);
            }
        }

        return base;
    }

    public boolean isSkippedByConnectorUnderTest(Description description,
                                                 final SkipWhenConnectorUnderTest connectorUnderTestAnnotation) {
        boolean isConnectorUnderTest;
        switch (connectorUnderTestAnnotation.check()) {
            case EQUAL:
                isConnectorUnderTest = connectorUnderTestAnnotation.value().isEqualTo(description.getClassName());
                break;
            default:
                isConnectorUnderTest = false;
                break;
        }
        return isConnectorUnderTest;
    }

    private boolean isSkippedByDatabaseVersion(SkipWhenDatabaseVersion skipWhenDatabaseVersion) {

        final EqualityCheck equalityCheck = skipWhenDatabaseVersion.check();
        final int major = skipWhenDatabaseVersion.major();
        final int minor = skipWhenDatabaseVersion.minor();
        final int patch = skipWhenDatabaseVersion.patch();

        // Scans the class path for SkipWhenDatabaseVersionResolver implementations under io.debezium packages
        final Reflections reflections = new Reflections("io.debezium");
        Set<Class<? extends DatabaseVersionResolver>> resolvers = reflections.getSubTypesOf(DatabaseVersionResolver.class);
        Class<? extends DatabaseVersionResolver> resolverClass = resolvers.stream().findFirst().orElse(null);

        if (resolverClass != null) {
            try {
                final DatabaseVersionResolver resolver = resolverClass.getDeclaredConstructor().newInstance();
                DatabaseVersion dbVersion = resolver.getVersion();
                if (dbVersion != null) {
                    switch (equalityCheck) {
                        case LESS_THAN:
                            return dbVersion.isLessThan(major, minor, patch);
                        case LESS_THAN_OR_EQUAL:
                            return dbVersion.isLessThanEqualTo(major, minor, patch);
                        case EQUAL:
                            return dbVersion.isEqualTo(major, minor, patch);
                        case GREATER_THAN_OR_EQUAL:
                            return dbVersion.isGreaterThanEqualTo(major, minor, patch);
                        case GREATER_THAN:
                            return dbVersion.isGreaterThan(major, minor, patch);
                    }
                }
            }
            catch (Exception e) {
                // In the event that the class cannot be loaded, run the test.
                e.printStackTrace();
            }
        }

        return false;
    }
}
