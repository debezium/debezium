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
import java.util.stream.Stream;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;

import io.debezium.DebeziumException;
import io.debezium.util.Collect;

/**
 * Tests to verify mongodb utilities
 */
public class MongoUtilTest {

    @Test
    void shouldGetClusterDescription() {
        ClusterDescription expectedClusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                List.of());

        var client = mock(MongoClient.class);
        when(client.getClusterDescription()).thenReturn(expectedClusterDescription);

        var actualDescription = MongoUtils.clusterDescription(client);
        assertThat(actualDescription).isEqualTo(expectedClusterDescription);
    }

    @Test
    void shouldGetClusterDescriptionAfterForcedConnection() {
        ClusterDescription unknwonClusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.UNKNOWN,
                List.of());

        ClusterDescription expectedClusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                List.of());

        // > Mongodb may not connect right away which results in UNKNOWN cluster type.
        // > MongoUtil.clusterDescription() forces the connection by listing databases when needed
        @SuppressWarnings("unchecked")
        var iterable = (MongoIterable<String>) mock(MongoIterable.class);
        when(iterable.first()).thenReturn("name");

        var client = mock(MongoClient.class);
        when(client.getClusterDescription()).thenReturn(unknwonClusterDescription, expectedClusterDescription);
        when(client.listDatabaseNames()).thenReturn(iterable);

        var actualDescription = MongoUtils.clusterDescription(client);
        assertThat(actualDescription).isEqualTo(expectedClusterDescription);
    }

    @Test
    void shouldGetReplicaSetName() {
        var rsNames = Collect.arrayListOf(null, "rs0", "rs1");
        var addresses = Collect.arrayListOf(new ServerAddress("host0"),
                new ServerAddress("host1"),
                new ServerAddress("host2"));

        List<ServerDescription> serverDescriptions = List.of(
                ServerDescription.builder()
                        .address(addresses.get(0))
                        .state(ServerConnectionState.CONNECTING)
                        .exception(new MongoSocketOpenException("can't connect", addresses.get(0)))
                        .build(),
                ServerDescription.builder()
                        .address(addresses.get(1))
                        .state(ServerConnectionState.CONNECTED)
                        .setName(rsNames.get(1))
                        .build(),
                ServerDescription.builder()
                        .address(addresses.get(2))
                        .state(ServerConnectionState.CONNECTED)
                        .setName(rsNames.get(2)) // In reality servers will have the same rs name
                        .build());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                serverDescriptions);

        var actualRsName = MongoUtils.replicaSetName(clusterDescription);

        assertThat(actualRsName).hasValue(rsNames.get(1));
    }

    @Test
    void shouldNotGetReplicaSetName() {
        var address = new ServerAddress("host0");

        List<ServerDescription> serverDescriptions = List.of(
                ServerDescription.builder()
                        .address(address)
                        .state(ServerConnectionState.CONNECTING)
                        .exception(new MongoSocketOpenException("can't connect", address))
                        .build());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                serverDescriptions);

        var actualRsName = MongoUtils.replicaSetName(clusterDescription);

        assertThat(actualRsName).isEmpty();
    }

    static Stream<String> requiredImageMessages() {
        // Server wording from the versioned sources linked in MongoUtils.isRequiredImageMissing().
        return Stream.of(
                // Pre-image message in MongoDB 6.0.0, 7.0.0 and 8.0.0.
                "Change stream was configured to require a pre-image for all update, delete and replace events, "
                        + "but the pre-image was not found for event: ",
                // Post-image message in MongoDB 6.0.0.
                "Change stream was configured to require a post-image for all update, delete and replace events, "
                        + "but the post-image was not found for event: ",
                // Post-image message in MongoDB 7.0.0 and 8.0.0 (also used in later 6.0 releases).
                "Change stream was configured to require a post-image for all update events, "
                        + "but the post-image was not found for event: ");
    }

    @ParameterizedTest
    @MethodSource("requiredImageMessages")
    void shouldRecognizeRequiredImageErrorsAcrossServerVersions(String message) {
        final var response = errorResponse(47, message + "{operationType: \"update\", ns: {db: \"test\", coll: \"documents\"}}");
        assertThat(MongoUtils.isRequiredImageMissing(new MongoCommandException(response, new ServerAddress()))).isTrue();
        assertThat(MongoUtils.isRequiredImageMissing(new MongoQueryException(response, new ServerAddress()))).isTrue();
    }

    @ParameterizedTest
    @MethodSource("requiredImageMessages")
    void shouldRecognizeRequiredImagesBehindServerAndExceptionPrefixes(String message) {
        final var response = errorResponse(47, "PlanExecutor error during aggregation :: caused by :: " + message + "{}");
        final var error = new DebeziumException("Checking change stream",
                new MongoException("Wrapped failure", new MongoCommandException(response, new ServerAddress())));
        assertThat(MongoUtils.isRequiredImageMissing(error)).isTrue();
    }

    @ParameterizedTest
    @MethodSource("requiredImageMessages")
    void shouldRequireMatchingErrorCodeAndMessage(String message) {
        final var error = new MongoCommandException(errorResponse(91, message + "{}"), new ServerAddress());
        assertThat(MongoUtils.isRequiredImageMissing(error)).isFalse();
        assertThat(MongoUtils.isRequiredImageMissing(new IllegalStateException(message))).isFalse();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "No matching document found for query {} on namespace config.rangeDeletions" })
    void shouldNotClassifyUnrelatedOrMissingMessagesAsRequiredImages(String message) {
        assertThat(MongoUtils.isRequiredImageMissing(new MongoException(47, message))).isFalse();
    }

    @Test
    void shouldNotClassifyImageWordsInWrapperAsAnImageError() {
        final var cause = new MongoCommandException(errorResponse(47, "No matching document found for another operation"), new ServerAddress());
        final var error = new DebeziumException("Change stream was configured to require a pre-image", cause);
        assertThat(MongoUtils.isRequiredImageMissing(error)).isFalse();
    }

    @Test
    void shouldAcceptNullWhenCheckingForRequiredImageErrors() {
        assertThat(MongoUtils.isRequiredImageMissing(null)).isFalse();
    }

    private static BsonDocument errorResponse(int code, String message) {
        return new BsonDocument("ok", new BsonInt32(0))
                .append("code", new BsonInt32(code))
                .append("errmsg", new BsonString(message));
    }

}
