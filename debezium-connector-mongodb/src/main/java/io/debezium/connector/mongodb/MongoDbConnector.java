/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.CaptureMode;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.Threads;

/**
 * A Kafka Connect source connector that creates {@link MongoDbConnectorTask tasks} that replicate the context of one or more
 * MongoDB replica sets.
 *
 * <h2>Sharded Clusters</h2>
 * This connector is able to fully replicate the content of one <a href="https://docs.mongodb.com/manual/sharding/">sharded
 * MongoDB 3.2 cluster</a>. In this case, simply configure the connector with the host addresses of the configuration replica set.
 * When the connector starts, it will discover and replicate the replica set for each shard.
 *
 * <h2>Replica Set</h2>
 * The connector is able to fully replicate the content of one <a href="https://docs.mongodb.com/manual/replication/">MongoDB
 * 3.2 replica set</a>. (Older MongoDB servers may be work but have not been tested.) In this case, simply configure the connector
 * with the host addresses of the replica set. When the connector starts, it will discover the primary node and use it to
 * replicate the contents of the replica set.
 * <p>
 * If necessary, a {@link MongoDbConnectorConfig#AUTO_DISCOVER_MEMBERS configuration property} can be used to disable the
 * logic used to discover the primary node, an in this case the connector will use the first host address specified in the
 * configuration as the primary node. Obviously this may cause problems when the replica set elects a different node as the
 * primary, since the connector will continue to read the oplog using the same node that may no longer be the primary.
 *
 * <h2>Parallel Replication</h2>
 * The connector will concurrently and independently replicate each of the replica sets. When the connector is asked to
 * {@link #taskConfigs(int) allocate tasks}, it will attempt to allocate a separate task for each replica set. However, if the
 * maximum number of tasks exceeds the number of replica sets, then some tasks may replicate multiple replica sets. Note that
 * each task will use a separate thread to replicate each of its assigned replica sets.
 *
 * <h2>Initial Sync and Reading the Oplog</h2>
 * When a connector begins to replicate a sharded cluster or replica set for the first time, it will perform an <em>initial
 * sync</em> of the collections in the replica set by generating source records for each document in each collection. Only when
 * this initial sync completes successfully will the replication then use the replica set's primary node to read the oplog and
 * produce source records for each oplog event. The replication process records the position of each oplog event as an
 * <em>offset</em>, so that upon restart the replication process can use the last recorded offset to determine where in the
 * oplog it is to begin reading and processing events.
 *
 * <h2>Use of Topics</h2>
 * The connector will write to a separate topic all of the source records that correspond to a single collection. The topic will
 * be named "{@code <logicalName>.<databaseName>.<collectionName>}", where {@code <logicalName>} is set via the
 * "{@link MongoDbConnectorConfig#LOGICAL_NAME mongodb.name}" configuration property.
 *
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MongoDbConnectorConfig}.
 *
 * @author Randall Hauch
 */
public class MongoDbConnector extends SourceConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Configuration config;
    private ReplicaSetMonitorThread monitorThread;
    private MongoDbTaskContext taskContext;
    private ConnectionContext connectionContext;
    private ExecutorService replicaSetMonitorExecutor;

    public MongoDbConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MongoDbConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(MongoDbConnectorConfig.ALL_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }

        this.config = config;

        // Set up the replication context ...
        taskContext = new MongoDbTaskContext(config);
        this.connectionContext = taskContext.getConnectionContext();

        PreviousContext previousLogContext = taskContext.configureLoggingContext("conn");
        try {
            logger.info("Starting MongoDB connector and discovering replica set(s) at {}", connectionContext.hosts());

            // Set up and start the thread that monitors the members of all of the replica sets ...
            replicaSetMonitorExecutor = Threads.newSingleThreadExecutor(MongoDbConnector.class, taskContext.serverName(), "replica-set-monitor");
            ReplicaSetDiscovery monitor = new ReplicaSetDiscovery(taskContext);
            monitorThread = new ReplicaSetMonitorThread(monitor::getReplicaSets, connectionContext.pollInterval(),
                    Clock.SYSTEM, () -> taskContext.configureLoggingContext("disc"), this::replicaSetsChanged);
            replicaSetMonitorExecutor.execute(monitorThread);
            logger.info("Successfully started MongoDB connector, and continuing to discover changes in replica set(s) at {}", connectionContext.hosts());
        }
        finally {
            previousLogContext.restore();
        }
    }

    protected void replicaSetsChanged(ReplicaSets replicaSets) {
        if (logger.isInfoEnabled()) {
            logger.info("Requesting task reconfiguration due to new/removed replica set(s) for MongoDB with seeds {}", connectionContext.hosts());
            logger.info("New replica sets include:");
            replicaSets.onEachReplicaSet(replicaSet -> logger.info("  {}", replicaSet));
        }
        context.requestTaskReconfiguration();
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        PreviousContext previousLogContext = taskContext.configureLoggingContext("conn");
        try {
            if (config == null) {
                logger.error("Configuring a maximum of {} tasks with no connector configuration available", maxTasks);
                return Collections.emptyList();
            }

            // Partitioning the replica sets amongst the number of tasks ...
            List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
            ReplicaSets replicaSets = monitorThread.getReplicaSets(10, TimeUnit.SECONDS);
            if (replicaSets != null) {
                validateReplicaSets(config.getString(MongoDbConnectorConfig.HOSTS), replicaSets);
                logger.info("Subdividing {} MongoDB replica set(s) into at most {} task(s)",
                        replicaSets.replicaSetCount(), maxTasks);
                replicaSets.subdivide(maxTasks, replicaSetsForTask -> {
                    // Create the configuration for each task ...
                    int taskId = taskConfigs.size();
                    logger.info("Configuring MongoDB connector task {} to capture events for replica set(s) at {}", taskId, replicaSetsForTask.hosts());
                    taskConfigs.add(config.edit()
                            .with(MongoDbConnectorConfig.HOSTS, replicaSetsForTask.hosts())
                            .with(MongoDbConnectorConfig.TASK_ID, taskId)
                            .build()
                            .asMap());
                });
            }
            logger.debug("Configuring {} MongoDB connector task(s)", taskConfigs.size());
            return taskConfigs;
        }
        finally {
            previousLogContext.restore();
        }
    }

    @Override
    public void stop() {
        PreviousContext previousLogContext = taskContext != null ? taskContext.configureLoggingContext("conn") : null;
        try {
            logger.info("Stopping MongoDB connector");
            this.config = null;
            // Clear interrupt flag so the graceful termination is always attempted.
            Thread.interrupted();
            if (replicaSetMonitorExecutor != null) {
                replicaSetMonitorExecutor.shutdownNow();
            }
            try {
                if (this.connectionContext != null) {
                    this.connectionContext.shutdown();
                }
            }
            finally {
                logger.info("Stopped MongoDB connector");
            }
        }
        finally {
            if (previousLogContext != null) {
                previousLogContext.restore();
            }
        }
    }

    @Override
    public ConfigDef config() {
        return MongoDbConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        final Configuration config = Configuration.from(connectorConfigs);

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = config.validate(MongoDbConnectorConfig.EXPOSED_FIELDS);

        // Get the config values for each of the connection-related fields ...
        ConfigValue hostsValue = results.get(MongoDbConnectorConfig.HOSTS.name());
        ConfigValue userValue = results.get(MongoDbConnectorConfig.USER.name());
        ConfigValue passwordValue = results.get(MongoDbConnectorConfig.PASSWORD.name());

        // If there are no errors on any of these ...
        if (hostsValue.errorMessages().isEmpty()
                && userValue.errorMessages().isEmpty()
                && passwordValue.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            try (ConnectionContext connContext = new ConnectionContext(config)) {
                try (MongoClient client = connContext.clientFor(connContext.hosts())) {
                    final MongoIterable<String> databaseNames = client.listDatabaseNames();
                    // Can't use 'local' database through mongos
                    final String databaseName = MongoUtil.contains(databaseNames, ReplicaSetDiscovery.CONFIG_DATABASE_NAME)
                            ? ReplicaSetDiscovery.CONFIG_DATABASE_NAME
                            : "local";
                    // Oplog mode is not supported for MongoDB 5+
                    // The version string format is not guaranteed so defensive measures are in place
                    final Document versionDocument = client.getDatabase(databaseName)
                            .runCommand(new Document("buildInfo", 1));
                    if (versionDocument != null) {
                        final String versionString = versionDocument.getString("version");
                        if (versionString != null) {
                            final String[] versionComponents = versionString.split("\\.");
                            if (versionComponents.length > 0) {
                                try {
                                    final int majorVersion = Integer.parseInt(versionComponents[0]);
                                    final ConfigValue captureModeValue = results
                                            .get(MongoDbConnectorConfig.CAPTURE_MODE.name());
                                    final MongoDbConnectorConfig connectorConfig = new MongoDbConnectorConfig(config);
                                    final CaptureMode captureMode = connectorConfig.getCaptureMode();
                                    if (majorVersion >= 5
                                            && captureMode == CaptureMode.OPLOG) {
                                        captureModeValue.addErrorMessage(
                                                "The 'oplog' capture mode is not supported for MongoDB 5 and newer; Please use 'change_streams'  or 'change_streams_update_full' instead");
                                    }
                                }
                                catch (NumberFormatException e) {
                                    // Ignore the exception
                                }
                            }
                        }
                    }
                }
            }
            catch (MongoException e) {
                hostsValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }

    /**
     * Validate mongodb.hosts config against the actual replica set obtained from the connection.
     * If the target is a replica set server (non-standalone) but mongodb.hosts config doesn't contain a leading
     * replica set name, the validation will also fail and this method will throw an exception.
     *
     * @param hosts mongodb.hosts config in string, e.g. rs/1.2.3.4:27017.
     * @param actualReplicaSets the actual replica set
     */
    protected void validateReplicaSets(String hosts, ReplicaSets actualReplicaSets) {
        if (hosts == null) {
            throw new ConnectException(
                    "The mongodb.hosts config should not be null for validation");
        }
        if (actualReplicaSets == null) {
            throw new ConnectException(
                    "The actual replica set should not be null for validation");
        }
        ReplicaSets expectedReplicaSets = ReplicaSets.parse(hosts);
        logger.info("Validating replica set names. Expected: {}, Actual: {}", expectedReplicaSets, actualReplicaSets);

        // Compare the replica set names only.
        List<String> expected = expectedReplicaSets.validReplicaSets().stream().map(ReplicaSet::replicaSetName).sorted().collect(Collectors.toList());
        List<String> actual = actualReplicaSets.validReplicaSets().stream().map(ReplicaSet::replicaSetName).sorted().collect(Collectors.toList());

        if (!expected.equals(actual)) {
            throw new ConnectException(
                    "Unexpected replica set names in server descriptions. Expected: " + expected + ", Got: " + actual);
        }
    }
}
