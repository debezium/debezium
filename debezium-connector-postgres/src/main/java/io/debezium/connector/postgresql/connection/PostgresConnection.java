/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresType;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

/**
 * {@link JdbcConnection} connection extension used for connecting to Postgres instances.
 *
 * @author Horia Chiorean
 */
public class PostgresConnection extends JdbcConnection {

    private static final String URL_PATTERN = "jdbc:postgresql://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
    protected static ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
                                                                                    org.postgresql.Driver.class.getName(),
                                                                                    PostgresConnection.class.getClassLoader());
    private static Logger LOGGER = LoggerFactory.getLogger(PostgresConnection.class);

    private static final String SQL_NON_ARRAY_TYPES = "SELECT t.oid AS oid, t.typname AS name "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A'";
    private static final String SQL_ARRAY_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typelem AS element "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory = 'A'";

    private final TypeRegistry typeRegistry;

    /**
     * Creates a Postgres connection using the supplied configuration.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public PostgresConnection(Configuration config) {
        super(config, FACTORY, PostgresConnection::validateServerVersion, PostgresConnection::defaultSettings);

        try {
            typeRegistry = initTypeRegistry(connection(), readTypeInfo());
        }
        catch (SQLException e) {
            throw new ConnectException("Could not intialize type registry", e);
        }
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        return connectionString(URL_PATTERN);
    }

    /**
     * Executes a series of statements without explicitly committing the connection.
     *
     * @param statements a series of statements to execute
     * @return this object so methods can be chained together; never null
     * @throws SQLException if anything fails
     */
    public PostgresConnection executeWithoutCommitting(String... statements) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement()) {
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        }
        return this;
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the given table
     */
    public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement = "SELECT relreplident FROM pg_catalog.pg_class c " +
                "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                replIdentity.append(rs.getString(1));
            } else {
                LOGGER.warn("Cannot determine REPLICA IDENTITY information for table '{}'", tableId);
            }
        });
        return ServerInfo.ReplicaIdentity.parseFromDB(replIdentity.toString());
    }

    protected ServerInfo.ReplicationSlot readReplicationSlotInfo(String slotName, String pluginName) throws SQLException {
        AtomicReference<ServerInfo.ReplicationSlot> replicationSlotInfo = new AtomicReference<>();
        String database = database();
        prepareQuery(
             "select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
                 statement.setString(1, slotName);
                 statement.setString(2, database);
                 statement.setString(3, pluginName);
             },
             rs -> {
                 if (rs.next()) {
                     boolean active = rs.getBoolean("active");
                     Long confirmedFlushedLSN = parseConfirmedFlushLsn(slotName, pluginName, database, rs);
                     replicationSlotInfo.compareAndSet(null, new ServerInfo.ReplicationSlot(active, confirmedFlushedLSN));
                 }
                 else {
                     LOGGER.debug("No replication slot '{}' is present for plugin '{}' and database '{}'", slotName,
                                  pluginName, database);
                     replicationSlotInfo.compareAndSet(null, ServerInfo.ReplicationSlot.INVALID);
                 }
             }
        );

        return replicationSlotInfo.get();
    }

    private Long parseConfirmedFlushLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Long confirmedFlushedLsn = null;

        try {
            String confirmedFlushLSNString = rs.getString("confirmed_flush_lsn");
            if (confirmedFlushLSNString == null) {
                throw new ConnectException("Value confirmed_flush_lsn is missing from the pg_replication_slots table for slot = '"
                        + slotName + "', plugin = '"
                        + pluginName + "', database = '"
                        + database + "'. This is an abnormal situation and the database status should be checked.");
            }
            try {
                confirmedFlushedLsn = LogSequenceNumber.valueOf(confirmedFlushLSNString).asLong();
            }
            catch (Exception e) {
                throw new ConnectException("Value confirmed_flush_lsn in the pg_replication_slots table for slot = '"
                        + slotName + "', plugin = '"
                        + pluginName + "', database = '"
                        + database + "' is not valid. This is an abnormal situation and the database status should be checked.");
            }
            if (confirmedFlushedLsn == LogSequenceNumber.INVALID_LSN.asLong()) {
                throw new ConnectException("Invalid LSN returned from database");
            }
         }
        catch (SQLException e) {
            // info not available, so we must be prior to PG 9.6
        }

        return confirmedFlushedLsn;
    }

    /**
     * Drops a replication slot that was created on the DB
     *
     * @param slotName the name of the replication slot, may not be null
     * @return {@code true} if the slot was dropped, {@code false} otherwise
     */
    public boolean dropReplicationSlot(String slotName) {
        try {
            execute("select pg_drop_replication_slot('" + slotName + "')");
            return true;
        } catch (SQLException e) {
            // slot is active
            PSQLState currentState = new PSQLState(e.getSQLState());
            if (PSQLState.OBJECT_IN_USE.equals(currentState)) {
                LOGGER.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                return false;
            } else if (PSQLState.UNDEFINED_OBJECT.equals(currentState)) {
                LOGGER.debug("Replication slot {} has already been dropped", slotName);
                return false;
            } else {
                LOGGER.error("Unexpected error while attempting to drop replication slot", e);
            }
            return false;
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        } catch (SQLException e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
    }

    /**
     * Returns the PG id of the current active transaction
     *
     * @return a PG transaction identifier, or null if no tx is active
     * @throws SQLException if anything fails.
     */
    public Long currentTransactionId() throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        query("select * from txid_current()", rs -> {
            if (rs.next()) {
                txId.compareAndSet(0, rs.getLong(1));
            }
        });
        long value = txId.get();
        return value > 0 ? value : null;
    }

    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    public long currentXLogLocation() throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        query(majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()", rs -> {
            if (!rs.next()) {
                throw new IllegalStateException("there should always be a valid xlog position");
            }
            result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
        });
        return result.get();
    }

    /**
     * Returns information about the PG server to which this instance is connected.
     *
     * @return a {@link ServerInfo} instance, never {@code null}
     * @throws SQLException if anything fails
     */
    public ServerInfo serverInfo() throws SQLException {
        ServerInfo serverInfo = new ServerInfo();
        query("SELECT version(), current_user, current_database()", rs -> {
            if (rs.next()) {
                serverInfo.withServer(rs.getString(1)).withUsername(rs.getString(2)).withDatabase(rs.getString(3));
            }
        });
        String username = serverInfo.username();
        if (username != null) {
            query("SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication FROM pg_roles " +
                    "WHERE pg_has_role('" + username + "', oid, 'member')",
                  rs -> {
                      while (rs.next()) {
                          String roleInfo = "superuser: " + rs.getBoolean(3) + ", replication: " + rs.getBoolean(8) +
                                  ", inherit: " + rs.getBoolean(4) + ", create role: " + rs.getBoolean(5) +
                                  ", create db: " + rs.getBoolean(6) + ", can log in: " + rs.getBoolean(7);
                          String roleName = rs.getString(2);
                          serverInfo.addRole(roleName, roleInfo);
                      }
                  });
        }
        return serverInfo;
    }

    protected static void defaultSettings(Configuration.Builder builder) {
        // we require Postgres 9.4 as the minimum server version since that's where logical replication was first introduced
        builder.with("assumeMinServerVersion", "9.4");
    }

    private static void validateServerVersion(Statement statement) throws SQLException {
        DatabaseMetaData metaData = statement.getConnection().getMetaData();
        int majorVersion = metaData.getDatabaseMajorVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        if (majorVersion < 9 || (majorVersion == 9 && minorVersion < 4)) {
            throw new SQLException("Cannot connect to a version of Postgres lower than 9.4");
        }
    }

    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getOid();
    }

    private static TypeRegistry initTypeRegistry(Connection db, Map<String, Integer> nameToJdbc) {
        final TypeInfo typeInfo = ((BaseConnection)db).getTypeInfo();
        TypeRegistry.Builder typeRegistryBuilder = TypeRegistry.create(typeInfo);
        try {
            try (final Statement statement = db.createStatement()) {
                // Read non-array types
                try (final ResultSet rs = statement.executeQuery(SQL_NON_ARRAY_TYPES)) {
                    while (rs.next()) {
                        final int oid = rs.getInt("oid");
                        typeRegistryBuilder.addType(new PostgresType(
                                rs.getString("name"),
                                oid,
                                nameToJdbc.get(rs.getString("name")),
                                typeInfo
                        ));
                    }
                }

                // Read array types
                try (final ResultSet rs = statement.executeQuery(SQL_ARRAY_TYPES)) {
                    while (rs.next()) {
                        // int2vector and oidvector will not be treated as arrays
                        final int oid = rs.getInt("oid");
                        typeRegistryBuilder.addType(new PostgresType(
                                rs.getString("name"),
                                oid,
                                nameToJdbc.get(rs.getString("name")),
                                typeInfo,
                                typeRegistryBuilder.get(rs.getInt("element"))
                        ));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Could not intialize type registry", e);
        }
        return typeRegistryBuilder.build();
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }
}
