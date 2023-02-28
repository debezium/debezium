/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;

import io.debezium.connector.mongodb.connection.ConnectionContext;

public class ReplicaSetDiscoveryTest {

    private ReplicaSetDiscovery replicaSetDiscovery;
    private MongoDbTaskContext context;
    private ConnectionContext connectionContext;
    private MongoClient mongoClient;

    @Before
    public void setup() {
        context = mock(MongoDbTaskContext.class);
        connectionContext = mock(ConnectionContext.class);
        mongoClient = mock(MongoClient.class);
        when(context.getConnectionContext()).thenReturn(connectionContext);
        when(connectionContext.connect()).thenReturn(mongoClient);
        replicaSetDiscovery = new ReplicaSetDiscovery(context);
    }

    /**
     * Testing the case where a member of a replicaset may be down.
     */
    @Test
    public void shouldGetFirstValidReplicaSetName() {
        // Skip the config server section of ReplicaSetDiscovery.getReplicaSets
        // by throwing an error here.
        when(mongoClient.listDatabaseNames()).thenThrow(new MongoException("dummy"));

        ServerAddress host1Address = new ServerAddress("host1");
        ServerAddress host2Address = new ServerAddress("host2");

        var cs = "mongodb://" + host1Address + "," + host1Address;
        when(connectionContext.connectionSeed()).thenReturn(cs);
        when(connectionContext.connectionString()).thenReturn(new ConnectionString(cs));

        List<ServerDescription> serverDescriptions = List.of(
                ServerDescription.builder()
                        .address(host1Address)
                        .state(ServerConnectionState.CONNECTING)
                        .exception(new MongoSocketOpenException("can't connect", host1Address))
                        .build(),
                ServerDescription.builder()
                        .address(host2Address)
                        .state(ServerConnectionState.CONNECTED)
                        .setName("my_rs")
                        .build());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                serverDescriptions);

        when(mongoClient.getClusterDescription()).thenReturn(clusterDescription);

        ReplicaSets replicaSets = replicaSetDiscovery.getReplicaSets();
        assertThat(replicaSets.all().size()).isEqualTo(1);
        assertThat(replicaSets.all().get(0).replicaSetName()).isEqualTo("my_rs");
    }
}
