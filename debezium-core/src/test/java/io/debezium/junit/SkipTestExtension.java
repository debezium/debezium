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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.reflections.Reflections;

import io.debezium.junit.DatabaseVersionResolver.DatabaseVersion;
import io.debezium.util.JvmVersionUtil;
import io.debezium.util.Testing;

/**
 * JUnit 5 Extension that inspects the presence of various skip annotations on a test method or test class.
 * This is the JUnit 5 equivalent of {@link SkipTestRule}.
 *
 * Supported annotations:
 * - {@link SkipWhenJavaVersion}
 * - {@link SkipLongRunning}
 * - {@link SkipOnOS}
 * - {@link SkipWhenKafkaVersion}
 * - {@link SkipWhenConnectorUnderTest}
 * - {@link SkipWhenConnectorsUnderTest}
 * - {@link SkipWhenDatabaseVersion}
 * - {@link SkipWhenDatabaseVersions}
 *
 * @author Horia Chiorean
 */
public class SkipTestExtension extends AnnotationBasedExtension implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        // Check SkipWhenJavaVersion
        SkipWhenJavaVersion skipJavaVersionAnnotation = hasAnnotation(context, SkipWhenJavaVersion.class);
        if (skipJavaVersionAnnotation != null) {
            ConditionEvaluationResult result = checkJavaVersion(skipJavaVersionAnnotation, context);
            if (result.isDisabled()) {
                return result;
            }
        }

        // Check SkipLongRunning
        SkipLongRunning skipLongRunningAnnotation = hasAnnotation(context, SkipLongRunning.class);
        if (skipLongRunningAnnotation != null) {
            ConditionEvaluationResult result = checkLongRunning(skipLongRunningAnnotation, context);
            if (result.isDisabled()) {
                return result;
            }
        }

        // Check SkipOnOS
        SkipOnOS skipOnOSAnnotation = hasAnnotation(context, SkipOnOS.class);
        if (skipOnOSAnnotation != null) {
            ConditionEvaluationResult result = checkOS(skipOnOSAnnotation, context);
            if (result.isDisabled()) {
                return result;
            }
        }

        // Check SkipWhenKafkaVersion
        SkipWhenKafkaVersion skipWhenKafkaVersionAnnotation = hasAnnotation(context, SkipWhenKafkaVersion.class);
        if (skipWhenKafkaVersionAnnotation != null) {
            ConditionEvaluationResult result = checkKafkaVersion(skipWhenKafkaVersionAnnotation, context);
            if (result.isDisabled()) {
                return result;
            }
        }

        // Check SkipWhenConnectorUnderTest
        SkipWhenConnectorUnderTest connectorUnderTestAnnotation = hasAnnotation(context, SkipWhenConnectorUnderTest.class);
        if (connectorUnderTestAnnotation != null) {
            ConditionEvaluationResult result = checkConnectorUnderTest(connectorUnderTestAnnotation, context);
            if (result.isDisabled()) {
                return result;
            }
        }

        // Check SkipWhenConnectorsUnderTest
        SkipWhenConnectorsUnderTest connectorsUnderTestAnnotation = hasAnnotation(context, SkipWhenConnectorsUnderTest.class);
        if (connectorsUnderTestAnnotation != null) {
            for (SkipWhenConnectorUnderTest connector : connectorsUnderTestAnnotation.value()) {
                ConditionEvaluationResult result = checkConnectorUnderTest(connector, context);
                if (result.isDisabled()) {
                    return result;
                }
            }
        }

        // Check SkipWhenDatabaseVersions (multiple)
        SkipWhenDatabaseVersions skipWhenDatabaseVersions = hasAnnotation(context, SkipWhenDatabaseVersions.class);
        if (skipWhenDatabaseVersions != null) {
            for (SkipWhenDatabaseVersion skipWhenDatabaseVersion : skipWhenDatabaseVersions.value()) {
                ConditionEvaluationResult result = checkDatabaseVersion(skipWhenDatabaseVersion, context);
                if (result.isDisabled()) {
                    return result;
                }
            }
        }

        // Check SkipWhenDatabaseVersion (single)
        SkipWhenDatabaseVersion skipWhenDatabaseVersion = hasAnnotation(context, SkipWhenDatabaseVersion.class);
        if (skipWhenDatabaseVersion != null) {
            ConditionEvaluationResult result = checkDatabaseVersion(skipWhenDatabaseVersion, context);
            if (result.isDisabled()) {
                return result;
            }
        }

        // If none of the skip conditions matched, enable the test
        return ConditionEvaluationResult.enabled("No skip conditions matched");
    }

    private ConditionEvaluationResult checkJavaVersion(SkipWhenJavaVersion annotation, ExtensionContext context) {
        int checkedVersion = annotation.value();
        int actualVersion = JvmVersionUtil.getFeatureVersion();
        boolean isSkippedVersion = false;

        switch (annotation.check()) {
            case EQUAL:
                isSkippedVersion = actualVersion == checkedVersion;
                break;
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
        }

        if (isSkippedVersion) {
            String message = formatSkipMessage("Java version=" + actualVersion, context);
            System.out.println(message);
            return ConditionEvaluationResult.disabled(message);
        }

        return ConditionEvaluationResult.enabled("Java version check passed");
    }

    private ConditionEvaluationResult checkLongRunning(SkipLongRunning annotation, ExtensionContext context) {
        String skipLongRunning = System.getProperty(SkipLongRunning.SKIP_LONG_RUNNING_PROPERTY);
        if (skipLongRunning == null || Boolean.valueOf(skipLongRunning)) {
            String message = formatSkipMessage(annotation.value(), context);
            System.out.println(message);
            return ConditionEvaluationResult.disabled(message);
        }

        return ConditionEvaluationResult.enabled("Long running tests are enabled");
    }

    private ConditionEvaluationResult checkOS(SkipOnOS annotation, ExtensionContext context) {
        String[] oses = annotation.value();
        String osName = System.getProperty("os.name");
        if (osName != null && !osName.trim().isEmpty()) {
            for (String os : oses) {
                if (osName.toLowerCase().startsWith(os.toLowerCase())) {
                    String message = formatSkipMessage(annotation.description(), context);
                    System.out.println(message);
                    return ConditionEvaluationResult.disabled(message);
                }
            }
        }

        return ConditionEvaluationResult.enabled("OS check passed");
    }

    private ConditionEvaluationResult checkKafkaVersion(SkipWhenKafkaVersion annotation, ExtensionContext context) {
        SkipWhenKafkaVersion.KafkaVersion kafkaVersion = annotation.value();
        EqualityCheck check = annotation.check();

        try (InputStream stream = Testing.class.getResourceAsStream("/kafka/kafka-version.properties")) {
            if (stream != null) {
                final Properties properties = new Properties();
                properties.load(stream);

                final String version = properties.getProperty("version");
                final String[] versionArray = version.split("\\.");

                int major = (versionArray.length >= 1) ? Integer.parseInt(versionArray[0]) : 0;
                int minor = (versionArray.length >= 2) ? Integer.parseInt(versionArray[1]) : 1;
                int patch = (versionArray.length >= 3) ? Integer.parseInt(versionArray[2]) : 2;

                boolean shouldSkip = false;
                switch (check) {
                    case LESS_THAN:
                        shouldSkip = kafkaVersion.isLessThan(major, minor, patch);
                        break;
                    case LESS_THAN_OR_EQUAL:
                        shouldSkip = kafkaVersion.isLessThanOrEqualTo(major, minor, patch);
                        break;
                    case EQUAL:
                        shouldSkip = kafkaVersion.isEqualTo(major, minor, patch);
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        shouldSkip = kafkaVersion.isGreaterThanOrEqualTo(major, minor, patch);
                        break;
                    case GREATER_THAN:
                        shouldSkip = kafkaVersion.isGreaterThan(major, minor, patch);
                        break;
                }

                if (shouldSkip) {
                    String message = formatSkipMessage(annotation.description(), context);
                    System.out.println(message);
                    return ConditionEvaluationResult.disabled(message);
                }
            }
        }
        catch (IOException e) {
            // In the event of an error determining the kafka version, run test.
        }

        return ConditionEvaluationResult.enabled("Kafka version check passed");
    }

    private ConditionEvaluationResult checkConnectorUnderTest(SkipWhenConnectorUnderTest annotation, ExtensionContext context) {
        boolean isSkippedConnector = isSkippedByConnectorUnderTest(context, annotation);

        if (isSkippedConnector) {
            String message = formatSkipMessage("Connector under test " + annotation.value(), context);
            System.out.println(message);
            return ConditionEvaluationResult.disabled(message);
        }

        return ConditionEvaluationResult.enabled("Connector check passed");
    }

    private boolean isSkippedByConnectorUnderTest(ExtensionContext context, SkipWhenConnectorUnderTest annotation) {
        boolean isConnectorUnderTest = false;
        switch (annotation.check()) {
            case EQUAL:
                String className = context.getTestClass().map(Class::getName).orElse("");
                isConnectorUnderTest = annotation.value().isEqualTo(className);
                break;
        }
        return isConnectorUnderTest;
    }

    private ConditionEvaluationResult checkDatabaseVersion(SkipWhenDatabaseVersion annotation, ExtensionContext context) {
        if (isSkippedByDatabaseVersion(annotation)) {
            String message = formatSkipMessage(annotation.reason(), context);
            System.out.println(message);
            return ConditionEvaluationResult.disabled(message);
        }

        return ConditionEvaluationResult.enabled("Database version check passed");
    }

    private boolean isSkippedByDatabaseVersion(SkipWhenDatabaseVersion skipWhenDatabaseVersion) {
        final EqualityCheck equalityCheck = skipWhenDatabaseVersion.check();
        final int major = skipWhenDatabaseVersion.major();
        final int minor = skipWhenDatabaseVersion.minor();
        final int patch = skipWhenDatabaseVersion.patch();

        // Scans the class path for DatabaseVersionResolver implementations under io.debezium packages
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
