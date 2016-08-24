/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import io.debezium.annotation.ThreadSafe;

/**
 * A connection pool of MongoClient instances. This pool supports creating clients that communicate explicitly with a single
 * server, or clients that communicate with any members of a replica set or sharded cluster given a set of seed addresses.
 * 
 * @author Randall Hauch
 */
@ThreadSafe
public class MongoClients {

    /**
     * Obtain a builder that can be used to configure and {@link Builder#build() create} a connection pool.
     * 
     * @return the new builder; never null
     */
    public static Builder create() {
        return new Builder();
    }

    /**
     * Configures and builds a ConnectionPool.
     */
    public static class Builder {
        private final List<MongoCredential> credentials = new CopyOnWriteArrayList<>();
        private final MongoClientOptions.Builder optionBuilder = MongoClientOptions.builder();

        /**
         * Add the given {@link MongoCredential} for use when creating clients.
         * 
         * @param credential the credential; may be {@code null}, though this method does nothing if {@code null}
         * @return this builder object so methods can be chained; never null
         */
        public Builder withCredential(MongoCredential credential) {
            if (credential != null) credentials.add(credential);
            return this;
        }

        /**
         * Obtain the options builder for client connections.
         * 
         * @return the option builder; never null
         */
        public MongoClientOptions.Builder options() {
            return optionBuilder;
        }

        /**
         * Build the client pool that will use the credentials and options already configured on this builder.
         * 
         * @return the new client pool; never null
         */
        public MongoClients build() {
            return new MongoClients(optionBuilder.build(), credentials);
        }
    }

    private final Map<ServerAddress, MongoClient> directConnections = new ConcurrentHashMap<>();
    private final Map<List<ServerAddress>, MongoClient> connections = new ConcurrentHashMap<>();
    private final List<MongoCredential> credentials = new CopyOnWriteArrayList<>();
    private final MongoClientOptions options;

    private MongoClients(MongoClientOptions options, List<MongoCredential> credentials) {
        this.options = options;
        if (credentials != null) {
            credentials.forEach(this.credentials::add);
        }
    }
    
    /**
     * Clear out and close any open connections.
     */
    public void clear() {
        directConnections.values().forEach(MongoClient::close);
        connections.values().forEach(MongoClient::close);
        directConnections.clear();
        connections.clear();
    }

    /**
     * Obtain a direct client connection to the specified server. This is typically used to connect to a standalone server,
     * but it also can be used to obtain a client that will only use this server, even if the server is a member of a replica
     * set or sharded cluster.
     * <p>
     * The format of the supplied string is one of the following:
     * 
     * <pre>
     * host:port
     * host
     * </pre>
     * 
     * where {@code host} contains the resolvable hostname or IP address of the server, and {@code port} is the integral port
     * number. If the port is not provided, the {@link ServerAddress#defaultPort() default port} is used. If neither the host
     * or port are provided (or {@code addressString} is {@code null}), then an address will use the
     * {@link ServerAddress#defaultHost() default host} and {@link ServerAddress#defaultPort() default port}.
     * 
     * @param addressString the string that contains the host and port of the server
     * @return the MongoClient instance; never null
     */
    public MongoClient clientFor(String addressString) {
        return clientFor(MongoUtil.parseAddress(addressString));
    }

    /**
     * Obtain a direct client connection to the specified server. This is typically used to connect to a standalone server,
     * but it also can be used to obtain a client that will only use this server, even if the server is a member of a replica
     * set or sharded cluster.
     * 
     * @param address the address of the server to use
     * @return the MongoClient instance; never null
     */
    public MongoClient clientFor(ServerAddress address) {
        return directConnections.computeIfAbsent(address, this::directConnection);
    }

    /**
     * Obtain a client connection to the replica set or cluster. The supplied addresses are used as seeds, and once a connection
     * is established it will discover all of the members.
     * <p>
     * The format of the supplied string is one of the following:
     * 
     * <pre>
     * replicaSetName/host:port
     * replicaSetName/host:port,host2:port2
     * replicaSetName/host:port,host2:port2,host3:port3
     * host:port
     * host:port,host2:port2
     * host:port,host2:port2,host3:port3
     * </pre>
     * 
     * where {@code replicaSetName} is the name of the replica set, {@code host} contains the resolvable hostname or IP address of
     * the server, and {@code port} is the integral port number. If the port is not provided, the
     * {@link ServerAddress#defaultPort() default port} is used. If neither the host or port are provided (or
     * {@code addressString} is {@code null}), then an address will use the {@link ServerAddress#defaultHost() default host} and
     * {@link ServerAddress#defaultPort() default port}.
     * <p>
     * This method does not use the replica set name.
     * 
     * @param addressList the string containing a comma-separated list of host and port pairs, optionally preceded by a
     *            replica set name
     * @return the MongoClient instance; never null
     */
    public MongoClient clientForMembers(String addressList) {
        return clientForMembers(MongoUtil.parseAddresses(addressList));
    }

    /**
     * Obtain a client connection to the replica set or cluster. The supplied addresses are used as seeds, and once a connection
     * is established it will discover all of the members.
     * 
     * @param seeds the seed addresses
     * @return the MongoClient instance; never null
     */
    public MongoClient clientForMembers(ServerAddress... seeds) {
        List<ServerAddress> addresses = new ArrayList<>();
        for (ServerAddress seedAddress : seeds) {
            if (seedAddress != null) addresses.add(seedAddress);
        }
        return clientForMembers(addresses);
    }

    /**
     * Obtain a client connection to the replica set or cluster. The supplied addresses are used as seeds, and once a connection
     * is established it will discover all of the members.
     * 
     * @param seedAddresses the seed addresses
     * @return the MongoClient instance; never null
     */
    public MongoClient clientForMembers(List<ServerAddress> seedAddresses) {
        return connections.computeIfAbsent(seedAddresses, this::connection);
    }

    protected MongoClient directConnection(ServerAddress address) {
        return new MongoClient(address, credentials, options);
    }

    protected MongoClient connection(List<ServerAddress> addresses) {
        return new MongoClient(addresses, credentials, options);
    }
}
