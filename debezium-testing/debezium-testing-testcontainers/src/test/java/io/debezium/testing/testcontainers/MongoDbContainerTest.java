/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class MongoDbContainerTest {

    @Test
    @FixFor("debezium/dbz#2931")
    void shouldSetGlibcTunablesForEveryContainerType() {
        final List<MongoDbContainer.Builder> builders = List.of(
                MongoDbContainer.node(),
                MongoDbContainer.router("configdb/config1:27017"),
                MongoDbContainer.configServerNode(),
                MongoDbContainer.shardServerNode());

        for (final MongoDbContainer.Builder builder : builders) {
            try (MongoDbContainer container = builder.name("mongo").build()) {
                assertThat(container.getEnvMap()).containsEntry("GLIBC_TUNABLES", "glibc.pthread.rseq=1");
            }
        }
    }
}
