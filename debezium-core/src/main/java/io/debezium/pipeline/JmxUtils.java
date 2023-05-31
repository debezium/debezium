/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;

public class JmxUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmxUtils.class);
    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.%s:type=%s, context=%s, server=%s";

    public static void registerMXBean(Object mxBean, CommonConnectorConfig connectorConfig, String type, String context) {

        String jmxObjectName = getJmxObjectName(type, context, connectorConfig);
        try {
            ObjectName objectName = new ObjectName(jmxObjectName);
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (!server.isRegistered(objectName)) {
                server.registerMBean(mxBean, objectName);
            }
        }
        catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            e.printStackTrace();
        }
    }

    public static void unregisterBean(CommonConnectorConfig connectorConfig, String type, String context) {

        String jmxObjectName = getJmxObjectName(type, context, connectorConfig);
        try {
            ObjectName objectName = new ObjectName(jmxObjectName);
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        }
        catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
            LOGGER.info("Unable to unregister metrics MBean '{}' as it was not found", jmxObjectName);
        }
    }

    private static String getJmxObjectName(String type, String context, CommonConnectorConfig connectorConfig) {
        return String.format(JMX_OBJECT_NAME_FORMAT, connectorConfig.getContextName().toLowerCase(), type, context, connectorConfig.getLogicalName());
    }
}
