/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import io.debezium.config.CommonConnectorConfig;

public class JmxUtils {

    private static final String JMX_OBJECT_NAME_FORMAT = "debezium.%s:type=%s, server=%s";

    public static void registerMXBean(Object mxBean, CommonConnectorConfig connectorConfig, String objectNameType) {
        try {

            ObjectName objectName = new ObjectName(getJmxObjectName(connectorConfig, objectNameType));
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (!server.isRegistered(objectName)) {
                server.registerMBean(mxBean, objectName);
            }
        }
        catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            e.printStackTrace();
        }
    }

    private static String getJmxObjectName(CommonConnectorConfig connectorConfig, String objectNameType) {
        return String.format(JMX_OBJECT_NAME_FORMAT, connectorConfig.getContextName(), objectNameType, connectorConfig.getLogicalName());
    }
}
