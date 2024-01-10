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

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;
import io.debezium.connector.mongodb.connection.ConnectionContext;
import io.debezium.connector.mongodb.connection.ConnectionStrings;
import io.debezium.connector.mongodb.connection.MongoDbConnection;
import io.debezium.connector.mongodb.connection.ReplicaSet;

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
 * "{@link io.debezium.config.CommonConnectorConfig.TOPIC_PREFIX topic.prefix}" configuration property.
 *
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link MongoDbConnectorConfig}.
 *
 * @author Randall Hauch
 */
public class MongoDbConnector extends BaseSourceConnector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Configuration config;
    private ConnectionContext connectionContext;

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
            throw new DebeziumException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.config = config;
        this.connectionContext = new ConnectionContext(config);

        logger.info("Successfully started MongoDB connector, and continuing to discover changes in replica set(s) at {}", connectionContext.maskedConnectionSeed());
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (config == null) {
            logger.error("Configuring a maximum of {} tasks with no connector configuration available", maxTasks);
            return Collections.emptyList();
        }

        // Partitioning the replica sets amongst the number of tasks ...
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        var taskConnectionString = connectionContext.resolveTaskConnectionString();

        logger.info("Configuring MongoDB connector task to capture events for connections to: {}", ConnectionStrings.mask(taskConnectionString));
        taskConfigs.add(config.edit()
                .with(MongoDbConnectorConfig.TASK_CONNECTION_STRING, taskConnectionString)
                .with(MongoDbConnectorConfig.TASK_ID, 0)
                .build()
                .asMap());
        logger.debug("Configuring {} MongoDB connector task(s)", taskConfigs.size());
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("Stopping MongoDB connector");
        this.config = null;
        // Clear interrupt flag so the graceful termination is always attempted.
        Thread.interrupted();
        logger.info("Stopped MongoDB connector");
    }

    @Override
    public ConfigDef config() {
        return MongoDbConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        final Configuration config = Configuration.from(connectorConfigs);

        // First, validate all of the individual fields, which is easy since don't make any of the fields invisible ...
        Map<String, ConfigValue> results = validateAllFields(config);

        // Get the config values for each of the connection-related fields ...
        ConfigValue connectionStringValue = results.get(MongoDbConnectorConfig.CONNECTION_STRING.name());
        ConfigValue userValue = results.get(MongoDbConnectorConfig.USER.name());
        ConfigValue passwordValue = results.get(MongoDbConnectorConfig.PASSWORD.name());

        // If there are no errors on any of these ...
        if (userValue.errorMessages().isEmpty()
                && passwordValue.errorMessages().isEmpty()
                && connectionStringValue.errorMessages().isEmpty()) {
            // Try to connect to the database ...
            ConnectionContext connContext = new ConnectionContext(config);
            try (MongoClient client = connContext.connect()) {
                client.listDatabaseNames().first(); // only when we try to fetch results a connection gets established
            }
            catch (MongoException e) {
                connectionStringValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        return new Config(new ArrayList<>(results.values()));
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(MongoDbConnectorConfig.EXPOSED_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<CollectionId> getMatchingCollections(Configuration config) {
        try (MongoDbConnection connection = getConnection(config)) {
            return connection.collections();
        }
        catch (InterruptedException e) {
            throw new DebeziumException(e);
        }
    }

    private MongoDbConnection getConnection(Configuration config) {
        MongoDbTaskContext context = new MongoDbTaskContext(config);
        ReplicaSet replicaSet = new ReplicaSet(context.getConnectionContext().connectionString());
        return context.getConnectionContext().connect(context.getConnectionContext().connectionString(), context.filters(), (s, throwable) -> {
            throw new DebeziumException(s, throwable);
        });
    }
}
