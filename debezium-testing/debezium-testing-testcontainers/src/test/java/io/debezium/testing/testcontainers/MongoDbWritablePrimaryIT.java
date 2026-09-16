/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static io.debezium.testing.testcontainers.MongoDbReplicaSet.replicaSet;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClients;

import io.debezium.doc.FixFor;
import io.debezium.testing.testcontainers.util.DockerUtils;

class MongoDbWritablePrimaryIT {

    @BeforeAll
    static void beforeAll() {
        DockerUtils.enableFakeDnsIfRequired();
    }

    @AfterAll
    static void afterAll() {
        DockerUtils.disableFakeDns();
    }

    @Test
    @FixFor("debezium/dbz#2649")
    void shouldWaitForWritablePrimaryBeforeCreatingRootUser() throws Exception {
        final var cluster = replicaSet().memberCount(1).authEnabled(true).build();
        final var member = cluster.getMembers().get(0);
        // Keep the elected primary in drain mode, where hello advertises its address but it cannot accept writes.
        member.withCreateContainerCmdModifier(command -> command.withCmd("-c", command.getCmd()[1]
                + " --setParameter enableTestCommands=1"
                + " --setParameter 'failpoint.hangBeforeRSTLOnDrainComplete={\"mode\":\"alwaysOn\"}'"));
        final var executor = Executors.newSingleThreadExecutor();

        try {
            // Pull the image and start mongod before timing the replica set initialization.
            member.start();
            final var startup = executor.submit(cluster::start);
            try {
                await().atMost(30, SECONDS)
                        .ignoreException(IllegalStateException.class)
                        .until(() -> cluster.tryPrimary().isPresent());

                assertThat(member.eval("rs.hello()").path("isWritablePrimary").asBoolean()).isFalse();
                assertThatThrownBy(() -> startup.get(2, SECONDS)).isInstanceOf(TimeoutException.class);
            }
            finally {
                member.eval("db.adminCommand({configureFailPoint: 'hangBeforeRSTLOnDrainComplete', mode: 'off'})");
            }

            startup.get(30, SECONDS);
            assertThat(member.eval("rs.hello()").path("isWritablePrimary").asBoolean()).isTrue();
            try (var client = MongoClients.create(cluster.getConnectionString())) {
                assertThat(client.getDatabase("test").getCollection("writable_primary")
                        .insertOne(new Document("_id", 1)).wasAcknowledged()).isTrue();
            }
        }
        finally {
            executor.shutdownNow();
            // Startup may have failed before the replica set was marked as started.
            cluster.getMembers().forEach(MongoDbContainer::stop);
        }
    }

    @Test
    @FixFor("debezium/dbz#2649")
    void shouldWaitForWritablePrimaryWhenFirstMemberIsSecondary() {
        try (var cluster = replicaSet().memberCount(3).build()) {
            cluster.start();
            final var expectedPrimary = cluster.getMembers().get(1);
            expectedPrimary.eval("db.adminCommand({replSetStepUp: 1})");
            await().atMost(30, SECONDS)
                    .ignoreException(IllegalStateException.class)
                    .until(() -> cluster.tryPrimary().filter(primary -> primary == expectedPrimary).isPresent());

            cluster.awaitWritablePrimary();

            assertThat(cluster.getMembers().get(0).eval("rs.hello()").path("isWritablePrimary").asBoolean()).isFalse();
            assertThat(expectedPrimary.eval("rs.hello()").path("isWritablePrimary").asBoolean()).isTrue();
        }
    }
}
