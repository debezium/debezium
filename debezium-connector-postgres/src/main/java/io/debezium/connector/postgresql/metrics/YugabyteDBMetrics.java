package io.debezium.connector.postgresql.metrics;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.JmxUtils;
import io.debezium.util.Collect;
import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metrics class to instantiate metrics for the {@link io.debezium.connector.postgresql.YugabyteDBConnector}
 */
public class YugabyteDBMetrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBMetrics.class);
    private final ObjectName name;
    private volatile boolean registered = false;

    protected YugabyteDBMetrics(CdcSourceTaskContext taskContext, Map<String, String> tags) {
        tags.putAll(taskContext.getCustomMetricTags());
        this.name = metricName(taskContext.getConnectorType(), tags);
    }

    protected YugabyteDBMetrics(CommonConnectorConfig connectorConfig, String contextName, boolean multiPartitionMode) {
        String connectorType = connectorConfig.getContextName();
        String connectorName = connectorConfig.getLogicalName();
        if (multiPartitionMode) {
            Map<String, String> tags = Collect.linkMapOf(
                    "server", connectorName,
                    "task", connectorConfig.getTaskId(),
                    "context", contextName);
            tags.putAll(connectorConfig.getCustomMetricTags());
            this.name = metricName(connectorType, tags);
        }
        else {
            this.name = metricName(connectorType, connectorName, contextName, connectorConfig.getCustomMetricTags());
        }
    }

    /**
     * Registers a metrics MBean into the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void register() {

        JmxUtils.registerMXBean(name, this);
        // If the old metrics MBean is present then the connector will try to unregister it
        // upon shutdown.
        registered = true;
    }

    /**
     * Unregisters a metrics MBean from the platform MBean server.
     * The method is intentionally synchronized to prevent preemption between registration and unregistration.
     */
    public synchronized void unregister() {
        if (this.name != null && registered) {
            JmxUtils.unregisterMXBean(name);
            registered = false;
        }
    }

    protected ObjectName metricName(String connectorType, String connectorName, String contextName, Map<String, String> customTags) {
        Map<String, String> tags = Collect.linkMapOf("context", contextName, "server", connectorName);
        tags.putAll(customTags);
        return metricName(connectorType, tags);
    }

    /**
     * Create a JMX metric name for the given metric.
     * @return the JMX metric name
     */
    protected ObjectName metricName(String connectorType, Map<String, String> tags) {
        final String metricName = "debezium." + connectorType.toLowerCase() + ":type=connector-metrics,"
                + tags.entrySet().stream()
                .map(e -> e.getKey() + "=" + Sanitizer.jmxSanitize(e.getValue()))
                .collect(Collectors.joining(","));
        try {
            return new ObjectName(metricName);
        }
        catch (MalformedObjectNameException e) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }
}
