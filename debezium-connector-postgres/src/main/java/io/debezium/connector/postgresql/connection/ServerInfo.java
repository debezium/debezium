/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
     * Table REPLICA IDENTITY information.
     */
    public enum ReplicaIdentity {
        NOTHING("UPDATE and DELETE events will not contain any old values"), 
        FULL("UPDATE AND DELETE events will contain the previous values of all the columns"), 
        DEFAULT("UPDATE and DELETE events will contain previous values only for PK columns"), 
        INDEX("UPDATE and DELETE events will contain previous values only for columns present in the REPLICA IDENTITY index"),
        UNKNOWN("Unknown REPLICA IDENTITY");
        
        private String description;
    
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
        
        protected static ReplicaIdentity parseFromDB(String s) {
            switch (s) {
                case "n": 
                    return NOTHING;    
                case "d": 
                    return DEFAULT;
                case "i" :
                    return INDEX;
                case "f" : 
                    return FULL;
                default: 
                   return UNKNOWN;                
            }
        }
        
    }
    
    /**
     * Information about a server replication slot
     */
    protected static class ReplicationSlot {
        protected static final ReplicationSlot INVALID = new ReplicationSlot(false, null);
        
        private boolean active;
        private Long latestFlushedLSN;
        
        protected ReplicationSlot(boolean active, Long latestFlushedLSN) {
            this.active = active;
            this.latestFlushedLSN = latestFlushedLSN;
        }
        
        protected boolean active() {
            return active;
        }
        
        protected Long latestFlushedLSN() {
            return latestFlushedLSN;
        }
        
        protected boolean hasValidFlushedLSN() {
            return latestFlushedLSN != null;
        }
    }
}
