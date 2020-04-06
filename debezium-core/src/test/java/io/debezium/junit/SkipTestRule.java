/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

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
            SkipWhenKafkaVersion.EqualityCheck check = skipWhenKafkaVersionAnnotation.check();
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

        return base;
    }
}
