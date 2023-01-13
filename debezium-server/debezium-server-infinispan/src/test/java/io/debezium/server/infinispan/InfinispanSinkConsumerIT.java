/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.infinispan;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.awaitility.Awaitility;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

import io.debezium.server.DebeziumServer;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * @author vjuranek
 */
@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(InfinispanTestResourceLifecycleManager.class)
public class InfinispanSinkConsumerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanSinkConsumerIT.class);
    private static final int MESSAGE_COUNT = 4;

    static {
        Testing.Files.delete(InfinispanTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(InfinispanTestConfigSource.OFFSET_STORE_PATH);
    }

    @Inject
    DebeziumServer server;

    private DefaultCacheManager cacheManager;
    private Cache<String, String> cache;

    @Test
    public void testStreaming() throws Exception {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        String uri = String.format("hotrod://%s:%s@%s:%d",
                InfinispanTestConfigSource.USER_NAME,
                InfinispanTestConfigSource.PASSWORD,
                InfinispanTestResourceLifecycleManager.getHost(),
                InfinispanTestResourceLifecycleManager.getPort());
        LOGGER.info("Connected to Infinispan server using URI '{}'", uri);
        builder.uri(uri);
        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(builder.build());
        RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(InfinispanTestConfigSource.CACHE_NAME);

        assertThat(remoteCache.size()).isEqualTo(0);

        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            return remoteCache.size() == MESSAGE_COUNT;
        });

        assertThat(remoteCache.size()).isEqualTo(MESSAGE_COUNT);
    }
}
