/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.connector.postgresql.spi.SlotState;

/**
 * Information about a running Postgres instance.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class ServerInfo {

    private String server;
    private String username;
    private String database;
    private Map<String, String> permissionsByRoleName;

    protected ServerInfo() {
        this.permissionsByRoleName = new HashMap<>();
    }

    protected ServerInfo withServer(final String server) {
        this.server = server;
        return this;
    }

    protected ServerInfo withUsername(final String username) {
        this.username = username;
        return this;
    }

    protected ServerInfo withDatabase(final String database) {
        this.database = database;
        return this;
    }

    protected ServerInfo addRole(String roleName, String permissions) {
        permissionsByRoleName.put(roleName, permissions);
        return this;
    }

    /**
     * Returns information about the server machine
     *
     * @return a String, possibly null if info was not available
     */
    public String server() {
        return server;
    }

    /**
     * Returns the name of the user of a connection
     *
     * @return a String, possibly null if info was not available
     */
    public String username() {
        return username;
    }

    /**
     * Returns the name of a database for which a connection was established
     *
     * @return a String, possibly null if info was not available
     */
    public String database() {
        return database;
    }

    /**
     * Returns information about the role names and permissions of the current user
     *
     * @return a {@link Map} of role information, keyed by role name; never null but possibly empty
     */
    public Map<String, String> permissionsByRoleName() {
        return permissionsByRoleName;
    }

    @Override
    public String toString() {
        String lineSeparator = System.lineSeparator();
        String roles = permissionsByRoleName.entrySet()
                .stream()
                .map(entry -> "\trole '" + entry.getKey() + "' [" + entry.getValue() + "]")
                .collect(Collectors.joining(lineSeparator));

        return "user '" + username + "' connected to database '" + database + "' on " + server + " with roles:" + lineSeparator + roles;
    }

    /**
     * Information about a server replication slot
     */
    protected static class ReplicationSlot {
        protected static final ReplicationSlot INVALID = new ReplicationSlot(false, null, null, null, null);

        private boolean active;
        private Lsn latestFlushedLsn;
        private Lsn restartLsn;
        private Long catalogXmin;
        private Long restartCommitHT;

        protected ReplicationSlot(boolean active, Lsn latestFlushedLsn, Lsn restartLsn, Long catalogXmin, Long restartCommitHT) {
            this.active = active;
            this.latestFlushedLsn = latestFlushedLsn;
            this.restartLsn = restartLsn;
            this.catalogXmin = catalogXmin;
            this.restartCommitHT = restartCommitHT;
        }

        protected boolean active() {
            return active;
        }

        /**
         * Represents the `confirmed_flushed_lsn` field of the replication slot.
         *
         * This value represents the latest LSN that the logical replication
         * consumer has reported back to postgres.
         * @return the latestFlushedLsn
         */
        protected Lsn latestFlushedLsn() {
            return latestFlushedLsn;
        }

        /**
         * Represents the `restart_lsn` field of the replication slot.
         *
         * The restart_lsn will be the LSN the slot restarts from
         * in the event of the disconnect. This can be distinct from
         * the `confirmed_flushed_lsn` as the two pointers are moved
         * independently
         * @return the restartLsn
         */
        protected Lsn restartLsn() {
            return restartLsn;
        }

        protected Long catalogXmin() {
            return catalogXmin;
        }

        protected Long restartCommitHT() {
            return restartCommitHT;
        }

        protected boolean hasValidFlushedLsn() {
            return latestFlushedLsn != null;
        }

        protected SlotState asSlotState() {
            return new SlotState(latestFlushedLsn, restartLsn, catalogXmin, active, restartCommitHT);
        }

        @Override
        public String toString() {
            return "ReplicationSlot [active=" + active + ", latestFlushedLsn=" + latestFlushedLsn + ", catalogXmin=" + catalogXmin + "]";
        }
    }
}
