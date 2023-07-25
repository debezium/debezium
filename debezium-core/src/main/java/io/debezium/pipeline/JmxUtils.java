/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.kafka.common.utils.Sanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

public class JmxUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxUtils.class);
    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.%s:type=%s, context=%s, server=%s";

    // Total 1 minute attempting to retry metrics registration in case of errors
    private static final int REGISTRATION_RETRIES = 12;
    private static final Duration REGISTRATION_RETRY_DELAY = Duration.ofSeconds(5);

    public static void registerMXBean(ObjectName objectName, Object mxBean) {

        try {

            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.info("JMX not supported, bean '{}' not registered", objectName);
                return;
            }
            // During connector restarts it is possible that Kafka Connect does not manage
            // the lifecycle perfectly. In that case it is possible the old metric MBean is still present.
            // There will be multiple attempts executed to register new MBean.
            for (int attempt = 1; attempt <= REGISTRATION_RETRIES; attempt++) {
                try {
                    mBeanServer.registerMBean(mxBean, objectName);
                    break;
                }
                catch (InstanceAlreadyExistsException e) {
                    if (attempt < REGISTRATION_RETRIES) {
                        LOGGER.warn(
                                "Unable to register metrics as an old set with the same name exists, retrying in {} (attempt {} out of {})",
                                REGISTRATION_RETRY_DELAY, attempt, REGISTRATION_RETRIES);
                        final Metronome metronome = Metronome.sleeper(REGISTRATION_RETRY_DELAY, Clock.system());
                        metronome.pause();
                    }
                    else {
                        LOGGER.error("Failed to register metrics MBean, metrics will not be available");
                    }
                }
            }
        }
        catch (JMException | InterruptedException e) {
            throw new RuntimeException("Unable to register the MBean '" + objectName + "'", e);
        }
    }

    public static void registerMXBean(Object mxBean, CommonConnectorConfig connectorConfig, String type, String context) {

        String jmxObjectName = getManagementJmxObjectName(type, context, connectorConfig);
        try {
            ObjectName objectName = new ObjectName(jmxObjectName);

            registerMXBean(objectName, mxBean);
        }
        catch (MalformedObjectNameException e) {
            throw new RuntimeException("Unable to register the MBean '" + jmxObjectName + "'", e);
        }

    }

    public static void unregisterMXBean(ObjectName objectName) {
        try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.debug("JMX not supported, bean '{}' not registered", objectName);
                return;
            }
            try {
                mBeanServer.unregisterMBean(objectName);
            }
            catch (InstanceNotFoundException e) {
                LOGGER.info("Unable to unregister metrics MBean '{}' as it was not found", objectName);
            }
        }
        catch (JMException e) {
            throw new RuntimeException("Unable to unregister the MBean '" + objectName + "'", e);
        }
    }

    public static void unregisterMXBean(CommonConnectorConfig connectorConfig, String type, String context) {

        String jmxObjectName = getManagementJmxObjectName(type, context, connectorConfig);
        try {
            ObjectName objectName = new ObjectName(jmxObjectName);
            unregisterMXBean(objectName);
        }
        catch (MalformedObjectNameException e) {
            LOGGER.info("Unable to unregister metrics MBean '{}' as it was not found", jmxObjectName);
        }
    }

    private static String getManagementJmxObjectName(String type, String context, CommonConnectorConfig connectorConfig) {
        String tags = String.format(JMX_OBJECT_NAME_FORMAT, connectorConfig.getContextName().toLowerCase(), type, context, connectorConfig.getLogicalName());
        if (connectorConfig.getCustomMetricTags().size() > 0) {
            String customTags = connectorConfig.getCustomMetricTags().entrySet().stream()
                    .map(e -> e.getKey() + "=" + Sanitizer.jmxSanitize(e.getValue()))
                    .collect(Collectors.joining(","));
            tags += "," + customTags;
        }
        return tags;
    }
}
