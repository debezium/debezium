/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static com.codahale.metrics.MetricRegistry.name;
import static io.debezium.connector.cassandra.CassandraConnectorTask.METRIC_REGISTRY_INSTANCE;
import static io.debezium.connector.cassandra.network.SslContextFactory.createSslContext;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.annotations.VisibleForTesting;

import io.netty.handler.ssl.SslContext;

/**
 * A wrapper around Cassandra driver that is used to query Cassandra table and table schema.
 */
public class CassandraClient implements AutoCloseable {
    private static final LoadBalancingPolicy DEFAULT_POLICY = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build());

    private final Cluster cluster;
    private final Session session;

    public CassandraClient(CassandraConnectorConfig config) throws GeneralSecurityException, IOException {
        this(config, DEFAULT_POLICY);
    }

    @VisibleForTesting
    CassandraClient(CassandraConnectorConfig config, LoadBalancingPolicy lbPolicy) throws GeneralSecurityException, IOException {
        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(config.cassandraHosts())
                .withPort(config.cassandraPort())
                .withProtocolVersion(ProtocolVersion.V4)
                .withLoadBalancingPolicy(lbPolicy)
                // See https://docs.datastax.com/en/developer/java-driver/3.5/manual/metrics/#metrics-4-compatibility.
                .withoutJMXReporting();

        if (config.cassandraUsername() != null && config.cassandraPassword() != null) {
            builder.withCredentials(config.cassandraUsername(), config.cassandraPassword());
        }

        if (config.cassandraSslEnabled()) {
            SslContext sslContext = createSslContext(config.cassandraSslConfigPath());
            SSLOptions sslOptions = new RemoteEndpointAwareNettySSLOptions(sslContext);
            builder.withSSL(sslOptions);
        }

        cluster = builder.build();
        session = cluster.connect();

        registerClusterMetrics(cluster.getClusterName());
    }

    public List<TableMetadata> getCdcEnabledTableMetadataList() {
        return cluster.getMetadata().getKeyspaces().stream()
                .map(KeyspaceMetadata::getTables)
                .flatMap(Collection::stream)
                .filter(tm -> tm.getOptions().isCDC())
                .collect(Collectors.toList());
    }

    public TableMetadata getCdcEnabledTableMetadata(String keyspace, String table) {
        TableMetadata tm = cluster.getMetadata().getKeyspace(keyspace).getTable(table);
        return tm.getOptions().isCDC() ? tm : null;
    }

    public Set<Host> getHosts() {
        return cluster.getMetadata().getAllHosts();
    }

    public String getClusterName() {
        return cluster.getMetadata().getClusterName();
    }

    public boolean isQueryable() {
        return !cluster.isClosed() && !session.isClosed();
    }

    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    public ResultSet execute(String query) {
        return session.execute(query);
    }

    public ResultSet execute(String query, Object... values) {
        return session.execute(query, values);
    }

    public ResultSet execute(String query, Map<String, Object> values) {
        return session.execute(query, values);
    }

    public void shutdown() {
        if (!session.isClosed()) {
            session.close();
        }
        if (!cluster.isClosed()) {
            cluster.close();
        }
    }

    private void registerClusterMetrics(String prefix) {
        MetricRegistry clusterRegistry = cluster.getMetrics().getRegistry();
        clusterRegistry.getMetrics().forEach((key, value) -> METRIC_REGISTRY_INSTANCE.register(name(prefix, key), value));
    }

    @Override
    public void close() {
        shutdown();
    }
}
