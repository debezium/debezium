/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.relational.RelationalTableFilters;

/**
 * A Postgres logical streaming replication connection. Replication connections are established for a slot and a given plugin
 * and WAL offset tracking is specific to the [database name, plugin name, slot name] triple.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@NotThreadSafe
public interface ReplicationConnection extends AutoCloseable {

    /**
     * Opens a stream for reading logical replication changes from the last known position of the slot for which the connection
     * was opened. The last known position is based on the server's {@code confirmed_flush_lsn} value from the {@code pg_replication_slots}
     * table.
     * <p>
     * If there is no "last known value" (i.e. the connection is for a newly created slot) then the Postgres server will start
     * streaming changes from that last position reported during this connection's creation.
     * </p>
     *
     * @return a {@link PGReplicationStream} from which data is read; never null
     * @throws SQLException if there is a problem obtaining the replication stream
     */
    ReplicationStream startStreaming() throws SQLException, InterruptedException;

    /**
     * Opens a stream for reading logical replication changes from a given LSN position.
     * <p>
     * Note that it is possible for a server to have recycled old WAL segments (see the {@code wal_keep_segments} setting). If
     * that is the case, then even though a LSN number may be valid, the server will not stream back any changes because they
     * are not available.
     * </p>
     * @param offset a value representing the WAL sequence number where replication should start from; if the value
     * is {@code null} or negative, this behaves exactly like {@link #startStreaming()}.
     * @return a {@link PGReplicationStream} from which data is read; never null
     * @see org.postgresql.replication.LogSequenceNumber
     * @throws SQLException if anything fails
     */
    ReplicationStream startStreaming(Long offset) throws SQLException, InterruptedException;

    /**
     * Creates a new replication slot with the given option and returns the result of the command, which
     * may contain results (depending on postgres versions)
     *
     * @throws SQLException if anything fails
     */
    Optional<SlotCreationResult> createReplicationSlot() throws SQLException;

    /**
     *  Forces the connection to be created, is called by startStreaming, but can be called manually
     *  in cases where we want the connection but not to to start streaming yet.
     *
     *  Can be called multiple times
     */
    void initConnection() throws SQLException, InterruptedException;

    /**
     * Checks whether this connection is open or not
     *
     * @return {@code true} if this connection is open, {@code false} otherwise
     * @throws SQLException if anything unexpected fails
     */
    boolean isConnected() throws SQLException;

    /**
     * Creates a new {@link Builder} instance which can be used for creating replication connections.
     *
     * @param jdbcConfig a {@link Configuration} instance that contains JDBC settings; may not be null
     * @return a builder, never null
     */
    static Builder builder(Configuration jdbcConfig) {
        return new PostgresReplicationConnection.ReplicationConnectionBuilder(jdbcConfig);
    }

    /**
     * Formats a LSN long value as a String value which can be used for outputting user-friendly LSN values.
     *
     * @param lsn the LSN value
     * @return a String format of the value, never {@code null}
     */
    static String format(long lsn) {
        return LogSequenceNumber.valueOf(lsn).asString();
    }

    /**
     * A builder for {@link ReplicationConnection}
     */
    interface Builder {

        /**
         * Default replication settings
         */
        String DEFAULT_SLOT_NAME = "debezium";
        String DEFAULT_PUBLICATION_NAME = "dbz_publication";
        boolean DEFAULT_DROP_SLOT_ON_CLOSE = true;
        boolean DEFAULT_EXPORT_SNAPSHOT = false;

        /**
         * Sets the name for the PG logical replication slot
         *
         * @param slotName the name of the slot, may not be null.
         * @return this instance
         * @see #DEFAULT_SLOT_NAME
         */
        Builder withSlot(final String slotName);

        /**
         * Sets the publication name for the PG logical publication
         *
         * @param publicationName the name of the publication, may not be null.
         * @return this instance
         * @see #DEFAULT_PUBLICATION_NAME
         */
        Builder withPublication(final String publicationName);

        /**
         * Sets the publication tables to watch for the PG logical publication
         *
         * @param tableFilter the configured table filters
         * @return this instance
         * @see #config.getTableFilters()
         */
        Builder withTableFilter(final RelationalTableFilters tableFilter);

        /**
         * Sets the publication autocreate mode for the PG logical publication
         *
         * @param publicationAutocreateMode the name of the publication, may not be null.
         * @return this instance
         * @see #PostgresConnectorConfig.PublicationAutocreateMode.ALL_TABLES
         */
        Builder withPublicationAutocreateMode(final PostgresConnectorConfig.AutoCreateMode publicationAutocreateMode);

        /**
         * Sets the instance for the PG logical decoding plugin
         *
         * @param pluginName the name of the slot, may not be null.
         * @return this instance
         * @see #PROTOBUF_PLUGIN_NAME
         */
        Builder withPlugin(final PostgresConnectorConfig.LogicalDecoder plugin);

        /**
         * Whether or not to drop the replication slot once the replication connection closes
         *
         * @param dropSlotOnClose true if the slot should be dropped once the connection is closed, false otherwise
         * @return this instance
         * @see #DEFAULT_DROP_SLOT_ON_CLOSE
         */
        Builder dropSlotOnClose(final boolean dropSlotOnClose);

        /**
         * The number of milli-seconds the replication connection should periodically send updates to the server.
         *
         * @param statusUpdateInterval a duration; null or non-positive value causes Postgres' default to be applied
         * @return this instance
         */
        Builder statusUpdateInterval(final Duration statusUpdateInterval);

        Builder withTypeRegistry(TypeRegistry typeRegistry);

        /**
         * Sets the schema instance
         *
         * @param schema the schema, must not be null
         * @return this instance
         */
        Builder withSchema(PostgresSchema schema);

        /**
         * Optional parameters to pass to the logical decoder when the stream starts.
         *
         * @param streamParams String of key and value pairs declared with "=". Pairs are separated by ";".
         *                     Example: "add-tables=public.table,public.table2;include-lsn=true"
         * @return this instance
         * @see #STREAM_PARAMS
         */
        Builder streamParams(final String streamParams);

        /**
         * Whether or not to export the snapshot when creating the slot
         * @param exportSnapshot true if a snapshot should be exported, false if otherwise
         * @return this instance
         * @see #DEFAULT_EXPORT_SNAPSHOT
         */
        Builder exportSnapshotOnCreate(final boolean exportSnapshot);

        /**
         * Whether or not the snapshot is executed
         * @param doSnapshot true if a snapshot is going to be executed, false if otherwise
         * @return this instance
         */
        Builder doSnapshot(final boolean doSnapshot);

        /**
         * Creates a new {@link ReplicationConnection} instance
         * @return a connection, never null
         */
        ReplicationConnection build();
    }
}
