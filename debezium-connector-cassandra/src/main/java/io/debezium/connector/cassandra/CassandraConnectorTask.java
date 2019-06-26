/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.network.BuildInfoServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A task that reads Cassandra commit log in CDC directory and generate corresponding data
 * change events which will be emitted to Kafka. If the table has not been bootstrapped,
 * this task will also take a snapshot of existing data in the database and convert each row
 * into a change event as well.
 */
public class CassandraConnectorTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectorTask.class);

    public static final MetricRegistry METRIC_REGISTRY_INSTANCE = new MetricRegistry();

    private final CassandraConnectorConfig config;
    private CassandraConnectorContext taskContext;
    private ProcessorGroup processorGroup;
    private Server httpServer;
    private JmxReporter jmxReporter;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new CassandraConnectorConfigException("CDC config file is required");
        }

        String configPath = args[0];
        try (FileInputStream fis = new FileInputStream(configPath)) {
            Map<String, Object> props = new Yaml().load(fis);
            CassandraConnectorConfig config = new CassandraConnectorConfig(props);
            CassandraConnectorTask task = new CassandraConnectorTask(config);
            task.run();
        }
    }

    public CassandraConnectorTask(CassandraConnectorConfig config) {
        this.config = config;
    }

    void run() throws Exception {
        try {
            LOGGER.info("Initializing Cassandra connector task context ...");
            taskContext = new CassandraConnectorContext(config);

            LOGGER.info("Starting processor group ...");
            initProcessorGroup();
            processorGroup.start();

            LOGGER.info("Starting HTTP server ...");
            initHttpServer();
            httpServer.start();

            LOGGER.info("Starting JMX reporter ...");
            initJmxReporter(taskContext.getCassandraConnectorConfig().name());
            jmxReporter.start();

            while (processorGroup.isRunning()) {
                Thread.sleep(1000);
            }
        } finally {
            stopAll();
        }
    }

    private void stopAll() throws Exception {
        if (processorGroup != null) {
            processorGroup.terminate();
            LOGGER.info("Stopped processor group");
        }

        if (httpServer != null) {
            httpServer.stop();
            LOGGER.info("Stopped HTTP server");
        }

        if (jmxReporter != null) {
            jmxReporter.stop();
            LOGGER.info("Stopped JMX reporter");
        }

        if (taskContext != null) {
            taskContext.cleanUp();
            LOGGER.info("Cleaned up Cassandra connector task context");
        }
    }

    private void initHttpServer() {
        int httpPort = config.httpPort();
        LOGGER.info("HTTP port is {}", httpPort);
        httpServer = new Server(httpPort);

        ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.setContextPath("/");
        httpServer.setHandler(contextHandler);

        contextHandler.addServlet(new ServletHolder(new PingServlet()), "/ping");
        contextHandler.addServlet(new ServletHolder(new BuildInfoServlet(getBuildInfoMap(this.getClass()))), "/buildinfo");
        contextHandler.addServlet(new ServletHolder(new MetricsServlet(METRIC_REGISTRY_INSTANCE)), "/metrics");
        contextHandler.addServlet(new ServletHolder(new HealthCheckServlet(registerHealthCheck())), "/health");
    }

    private void initProcessorGroup() throws IOException {
        processorGroup = new ProcessorGroup("Cassandra Connector Processor Group");
        processorGroup.addProcessor(new SchemaProcessor(taskContext, processorGroup.taskState));
        processorGroup.addProcessor(new CommitLogProcessor(taskContext, processorGroup.taskState));
        processorGroup.addProcessor(new SnapshotProcessor(taskContext, processorGroup.taskState));
        processorGroup.addProcessor(new QueueProcessor(taskContext, processorGroup.taskState));
        if (taskContext.getCassandraConnectorConfig().postProcessEnabled()) {
            processorGroup.addProcessor(new CommitLogPostProcessor(taskContext, processorGroup.taskState));
        }
    }

    private void initJmxReporter(String domain) {
        jmxReporter = JmxReporter.forRegistry(METRIC_REGISTRY_INSTANCE).inDomain(domain).build();
    }

    private HealthCheckRegistry registerHealthCheck() {
        CassandraConnectorTaskHealthCheck healthCheck = new CassandraConnectorTaskHealthCheck(processorGroup, taskContext.getCassandraClient());
        HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
        healthCheckRegistry.register("cassandra-cdc-health-check", healthCheck);
        return healthCheckRegistry;
    }

    private static Map<String, String> getBuildInfoMap(Class<?> clazz) {
        Map<String, String> buildInfo = new HashMap<>();
        buildInfo.put("version", clazz.getPackage().getImplementationVersion());
        buildInfo.put("service_name", clazz.getPackage().getImplementationTitle());
        return buildInfo;
    }

    /**
     * A processor group consist of one or more processors; each processor will be running on a separate thread.
     * The processors are interdependent of one another and share the same 'taskState'. If one of the processors
     * is stopped, that processor will update the 'taskState' to false. and this will result in all other processors
     * in this processor group to be terminated eventually as each processor periodically checks this 'taskState'.
     */
    public static class ProcessorGroup {
        private final String name;
        private final AtomicBoolean taskState;
        private final Set<AbstractProcessor> processors;
        private ExecutorService executorService;

        ProcessorGroup(String name) {
            this.name = name;
            this.taskState = new AtomicBoolean(false);
            this.processors = new HashSet<>();
        }

        public boolean isRunning() {
            return taskState.get();
        }

        public String getName() {
            return name;
        }

        AtomicBoolean getTaskState() {
            return taskState;
        }

        Set<AbstractProcessor> getProcessors() {
            return processors;
        }

        void addProcessor(AbstractProcessor processor) {
            processors.add(processor);
        }

        void start() {
            executorService = Executors.newFixedThreadPool(getProcessors().size());
            LOGGER.info("Starting processor group {}", getName());
            for (AbstractProcessor processor : getProcessors()) {
                executorService.submit(createRunnable(processor));
            }
        }

        void terminate() throws InterruptedException {
            LOGGER.info("Stopping processor group {}", getName());
            executorService.shutdown();
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        }

        private <T extends AbstractProcessor> Runnable createRunnable(T processor) {
            return () -> {
                try {
                    processor.initialize();
                    processor.start();
                } catch (Exception e) {
                    LOGGER.error("Terminating Cassandra connector task. Encountered exception while running {}: {}", processor.getName(), e.getStackTrace());
                } finally {
                    try {
                        processor.stop();
                        processor.destroy();
                    } catch (Exception e) {
                        LOGGER.error("Encountered exception while stopping {}: {}", processor.getName(), e.getStackTrace());
                    }
                }
            };
        }
    }
}
