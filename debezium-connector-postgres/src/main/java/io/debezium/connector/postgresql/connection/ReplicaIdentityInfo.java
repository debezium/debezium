/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

/**
 * Class to encapsulate {@link ReplicaIdentityInfo.ReplicaIdentity} enumerator, adding the name of the index
 * in case of the Replica Identity was set to `INDEX`
 * @author Ben White, Miguel Sotomayor
 */
public class ReplicaIdentityInfo {

    private String indexName;
    private final ReplicaIdentity replicaIdentity;

    public ReplicaIdentityInfo(ReplicaIdentity replicaIdentity) {
        this.replicaIdentity = replicaIdentity;
        this.indexName = null;
    }

    public ReplicaIdentityInfo(ReplicaIdentity replicaIdentity, String indexName) {
        this(replicaIdentity);
        this.indexName = indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String description() {
        return this.replicaIdentity.description();
    }

    public ReplicaIdentity getReplicaIdentity() {
        return replicaIdentity;
    }

    @Override
    public String toString() {
        return indexName != null ? String.format("USING INDEX %s", indexName) : this.replicaIdentity.name();
    }

    /**
     * Table REPLICA IDENTITY information.
     */
    public enum ReplicaIdentity {
        // YB Note: CHANGE is a YugabyteDB specific replica identity.
        NOTHING("UPDATE and DELETE events will not contain any old values"),
        FULL("UPDATE AND DELETE events will contain the previous values of all the columns"),
        DEFAULT("UPDATE and DELETE events will contain previous values only for PK columns"),
        INDEX("UPDATE and DELETE events will contain previous values only for columns present in the REPLICA IDENTITY index"),
        UNKNOWN("Unknown REPLICA IDENTITY"),
        CHANGE("UPDATE events will contain values only for changed columns");

        private final String description;

        /**
         * Returns a textual description of the replica identity
         *
         * @return a description, never null
         */
        public String description() {
            return this.description;
        }

        ReplicaIdentity(String description) {
            this.description = description;
        }

        // YB Note: CHANGE is a YugabyteDB specific replica identity.
        public static ReplicaIdentityInfo.ReplicaIdentity parseFromDB(String s) {
            switch (s) {
                case "n":
                    return NOTHING;
                case "d":
                    return DEFAULT;
                case "i":
                    return INDEX;
                case "f":
                    return FULL;
                case "c":
                    return CHANGE;
                default:
                    return UNKNOWN;
            }
        }
    }
}