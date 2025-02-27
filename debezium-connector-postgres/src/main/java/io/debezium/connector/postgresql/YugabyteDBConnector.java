/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

/**
 * A Kafka Connect source connector that creates tasks which use Postgresql streaming replication off a logical replication slot
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in {@link PostgresConnectorConfig}.
 *
 * @author Horia Chiorean
 */
public class YugabyteDBConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnector.class);
    private Map<String, String> props;
    private PostgresConnectorConfig connectorConfig;

    public YugabyteDBConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        this.connectorConfig = new PostgresConnectorConfig(Configuration.from(props));
    }

    protected List<Map<String, String>> getTaskConfigsForParallelStreaming(List<String> slotNames,
                                                                           List<String> publicationNames,
                                                                           List<String> slotRanges) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        if (connectorConfig.getSnapshotter().shouldSnapshot()) {
            props.put(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name(), props.get(PostgresConnectorConfig.TABLE_INCLUDE_LIST.name()));
        }

        for (int i = 0; i < slotNames.size(); ++i) {
            Map<String, String> taskProps = new HashMap<>(this.props);

            taskProps.put(PostgresConnectorConfig.TASK_ID, String.valueOf(i));
            taskProps.put(PostgresConnectorConfig.SLOT_NAME.name(), slotNames.get(i));
            taskProps.put(PostgresConnectorConfig.PUBLICATION_NAME.name(), publicationNames.get(i));
            taskProps.put(PostgresConnectorConfig.STREAM_PARAMS.name(), "hash_range=" + slotRanges.get(i));

            if (connectorConfig.getSnapshotter().shouldSnapshot()) {
                String[] splitRange = slotRanges.get(i).split(",");
                String query = getParallelSnapshotQuery(splitRange[0], splitRange[1]);
                taskProps.put(
                    PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + "." + taskProps.get(PostgresConnectorConfig.TABLE_INCLUDE_LIST.name()),
                    query
                );
            }

            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (props == null) {
            return Collections.emptyList();
        }

        final String tableIncludeList = props.get(PostgresConnectorConfig.TABLE_INCLUDE_LIST.name());

        if (connectorConfig.streamingMode().isParallel()) {
            LOGGER.info("Initialising parallel streaming mode");

            // Validate for a single table.
            validateSingleTableProvided(tableIncludeList, false /* isSnapshot */);

            List<String> slotNames = connectorConfig.getSlotNames();
            List<String> publicationNames = connectorConfig.getPublicationNames();
            List<String> slotRanges = connectorConfig.getSlotRanges();

            YBValidate.slotAndPublicationsAreEqual(slotNames, publicationNames);
            YBValidate.slotRangesMatchSlotNames(slotNames, slotRanges);
            YBValidate.completeRangesProvided(slotRanges);

            return getTaskConfigsForParallelStreaming(slotNames, publicationNames, slotRanges);
        }

        // TODO Vaibhav (#26106): The following code block is not needed now, remove in a separate PR.
        if (props.containsKey(PostgresConnectorConfig.SNAPSHOT_MODE.name())
                && props.get(PostgresConnectorConfig.SNAPSHOT_MODE.name())
                    .equalsIgnoreCase(PostgresConnectorConfig.SnapshotMode.PARALLEL.getValue())) {
            LOGGER.info("Initialising parallel snapshot consumption");

            // Perform basic validations.
            validateSingleTableProvided(tableIncludeList, true);

            // Publication auto create mode should not be for all tables.
            if (props.containsKey(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name())
                    && props.get(PostgresConnectorConfig.PUBLICATION_AUTOCREATE_MODE.name())
                        .equalsIgnoreCase(PostgresConnectorConfig.AutoCreateMode.ALL_TABLES.getValue())) {
                throw new DebeziumException("Snapshot mode parallel is not supported with publication.autocreate.mode all_tables, " +
                                            "use publication.autocreate.mode=filtered");
            }

            // Add configuration for select override.
            props.put(PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name(), tableIncludeList);

            return getConfigForParallelSnapshotConsumption(maxTasks);
        }

        // YB Note: Only applicable when snapshot mode is not parallel.
        // this will always have just one task with the given list of properties

        // This is to ensure that whenever the connector runs with a single task model,
        // the default task ID is populated as this is not done by debezium-core.
        if (props != null) {
            props.put(PostgresConnectorConfig.TASK_ID, "0");
        }

        return props == null ? Collections.emptyList() : Collections.singletonList(new HashMap<>(props));
    }

    protected void validateSingleTableProvided(String tableIncludeList, boolean isSnapshot) throws DebeziumException {
        if (tableIncludeList == null) {
            throw new DebeziumException("No table provided, provide a table in the table.include.list");
        } else if (tableIncludeList.contains(",")) {
            // This might indicate the presence of multiple tables in the include list, we do not want that.
            throw new DebeziumException("parallel " + (isSnapshot ? "snapshot" : "streaming") + " consumption is only supported with one table at a time");
        }
    }

    // TODO Vaibhav (#26106): This method needs to be removed.
    protected List<Map<String, String>> getConfigForParallelSnapshotConsumption(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();

        final long upperBoundExclusive = 64 * 1024;
        final long rangeSize = upperBoundExclusive / maxTasks;

        for (int i = 0; i < maxTasks; ++i) {
            Map<String, String> taskProps = new HashMap<>(this.props);

            taskProps.put(PostgresConnectorConfig.TASK_ID, String.valueOf(i));

            long lowerBound = i * rangeSize;
            long upperBound = (i == maxTasks - 1) ? upperBoundExclusive - 1 : (lowerBound + rangeSize - 1);

            LOGGER.info("Using query for task {}: {}", i, getQueryForParallelSnapshotSelect(lowerBound, upperBound));

            taskProps.put(
              PostgresConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name() + "." + taskProps.get(PostgresConnectorConfig.TABLE_INCLUDE_LIST.name()),
              getQueryForParallelSnapshotSelect(lowerBound, upperBound)
            );

            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    protected String getQueryForParallelSnapshotSelect(long lowerBound, long upperBound) {
        return String.format("SELECT * FROM %s WHERE yb_hash_code(%s) >= %d AND yb_hash_code(%s) <= %d",
                    props.get(PostgresConnectorConfig.TABLE_INCLUDE_LIST.name()),
                    props.get(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS.name()), lowerBound,
                    props.get(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS.name()), upperBound);
    }

    // TODO Vaibhav (#26106): This is a copy of existing method, remove the older method in a separate PR.
    protected String getParallelSnapshotQuery(String lowerBound, String upperBound) {
        return String.format("SELECT * FROM %s WHERE yb_hash_code(%s) >= %s AND yb_hash_code(%s) < %s",
                props.get(PostgresConnectorConfig.TABLE_INCLUDE_LIST.name()),
                props.get(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS.name()), lowerBound,
                props.get(PostgresConnectorConfig.PRIMARY_KEY_HASH_COLUMNS.name()), upperBound);
    }

    @Override
    public void stop() {
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return PostgresConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        final ConfigValue slotNameValue = configValues.get(PostgresConnectorConfig.SLOT_NAME.name());
        final ConfigValue pluginNameValue = configValues.get(PostgresConnectorConfig.PLUGIN_NAME.name());
        if (!databaseValue.errorMessages().isEmpty() || !slotNameValue.errorMessages().isEmpty()
                || !pluginNameValue.errorMessages().isEmpty()) {
            return;
        }

        final PostgresConnectorConfig postgresConfig = new PostgresConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        try (PostgresConnection connection = new PostgresConnection(postgresConfig.getJdbcConfig(),
                PostgresConnection.CONNECTION_VALIDATE_CONNECTION, postgresConfig.ybShouldLoadBalanceConnections())) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                testConnection(connection, postgresConfig.ybShouldLoadBalanceConnections());

                // YB Note: This check validates that the WAL level is "logical" - skipping this
                //          since it is not applicable to YugabyteDB.
                if (!YugabyteDBServer.isEnabled()) {
                    checkWalLevel(connection, postgresConfig);
                }

                checkLoginReplicationRoles(connection);
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'",
                        connection.connectionString(postgresConfig.ybShouldLoadBalanceConnections()),
                                connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
            }
        }
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> connectorConfig) {
        return ExactlyOnceSupport.SUPPORTED;
    }

    private static void checkLoginReplicationRoles(PostgresConnection connection) throws SQLException {
        if (!connection.queryAndMap(
                "SELECT r.rolcanlogin AS rolcanlogin, r.rolreplication AS rolreplication," +
                // for AWS the user might not have directly the rolreplication rights, but can be assigned
                // to one of those role groups: rds_superuser, rdsadmin or rdsrepladmin
                        " CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rds_superuser') AS BOOL) IS TRUE AS aws_superuser" +
                        ", CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rdsadmin') AS BOOL) IS TRUE AS aws_admin" +
                        ", CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rdsrepladmin') AS BOOL) IS TRUE AS aws_repladmin" +
                        ", CAST(array_position(ARRAY(SELECT b.rolname" +
                        " FROM pg_catalog.pg_auth_members m" +
                        " JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)" +
                        " WHERE m.member = r.oid), 'rds_replication') AS BOOL) IS TRUE AS aws_replication" +
                        " FROM pg_roles r WHERE r.rolname = current_user",
                connection.singleResultMapper(rs -> rs.getBoolean("rolcanlogin")
                        && (rs.getBoolean("rolreplication")
                                || rs.getBoolean("aws_superuser")
                                || rs.getBoolean("aws_admin")
                                || rs.getBoolean("aws_repladmin")
                                || rs.getBoolean("aws_replication")),
                        "Could not fetch roles"))) {
            final String errorMessage = "Postgres roles LOGIN and REPLICATION are not assigned to user: " + connection.username();
            LOGGER.error(errorMessage);
        }
    }

    private static void checkWalLevel(PostgresConnection connection, PostgresConnectorConfig config) throws SQLException {
        final String walLevel = connection.queryAndMap(
                "SHOW wal_level",
                connection.singleResultMapper(rs -> rs.getString("wal_level"), "Could not fetch wal_level"));
        if (!"logical".equals(walLevel)) {
            if (config.getSnapshotter() != null && config.getSnapshotter().shouldStream()) {
                // Logical WAL_LEVEL is only necessary for CDC snapshotting
                throw new SQLException("Postgres server wal_level property must be 'logical' but is: '" + walLevel + "'");
            }
            else {
                LOGGER.warn("WAL_LEVEL check failed but this is ignored as CDC was not requested");
            }
        }
    }

    private static void testConnection(PostgresConnection connection, Boolean loadBalance) throws SQLException {
        connection.execute("SELECT version()");
        LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(loadBalance),
                connection.username());
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(PostgresConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(config);
        try (PostgresConnection connection = new PostgresConnection(connectorConfig.getJdbcConfig(),
                PostgresConnection.CONNECTION_GENERAL, connectorConfig.ybShouldLoadBalanceConnections())) {
            return connection.readTableNames(connectorConfig.databaseName(), null, null, new String[]{ "TABLE" }).stream()
                    .filter(tableId -> connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }
}
