/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.infinispan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;

/**
 * An implementation of the {@link DebeziumEngine.ChangeConsumer} interface that publishes change event messages to predefined Infinispan cache.
 *
 * @author vjuranek
 */
@Named("infinispan")
@Dependent
public class InfinispanSinkConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanSinkConsumer.class);

    private static final String CONF_PREFIX = "debezium.sink.infinispan.";
    private static final String SERVER_HOST = CONF_PREFIX + "server.host";
    private static final String SERVER_PORT = CONF_PREFIX + "server.port";
    private static final String CACHE_NAME = CONF_PREFIX + "cache";
    private static final String USER_NAME = CONF_PREFIX + "user";
    private static final String PASSWORD = CONF_PREFIX + "password";

    private RemoteCacheManager remoteCacheManager;
    private RemoteCache cache;

    @Inject
    @CustomConsumerBuilder
    Instance<RemoteCache> customCache;

    @PostConstruct
    void connect() {
        if (customCache.isResolvable()) {
            cache = customCache.get();
            LOGGER.info("Obtained custom cache with configuration '{}'", cache.getRemoteCacheContainer().getConfiguration());
            return;
        }

        final Config config = ConfigProvider.getConfig();
        final String serverHost = config.getValue(SERVER_HOST, String.class);
        final String cacheName = config.getValue(CACHE_NAME, String.class);
        final Integer serverPort = config.getOptionalValue(SERVER_PORT, Integer.class).orElse(ConfigurationProperties.DEFAULT_HOTROD_PORT);
        final Optional<String> user = config.getOptionalValue(USER_NAME, String.class);
        final Optional<String> password = config.getOptionalValue(PASSWORD, String.class);

        ConfigurationBuilder builder = new ConfigurationBuilder();
        String uri;
        if (user.isPresent() && password.isPresent()) {
            uri = String.format("hotrod://%s:%s@%s:%d", user.get(), password.get(), serverHost, serverPort);
        }
        else {
            uri = String.format("hotrod://%s:%d", serverHost, serverPort);
        }
        LOGGER.info("Connecting to the Infinispan server using URI '{}'", uri);
        builder.uri(uri);

        remoteCacheManager = new RemoteCacheManager(builder.build());
        cache = remoteCacheManager.getCache(cacheName);
        LOGGER.info("Connected to the Infinispan server {}", remoteCacheManager.getServers()[0]);
    }

    @PreDestroy
    void close() {
        try {
            if (remoteCacheManager != null) {
                remoteCacheManager.close();
                LOGGER.info("Connection to Infinispan server closed.");
            }
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        Map<Object, Object> entries = new HashMap<>(records.size());
        for (ChangeEvent<Object, Object> record : records) {
            if (record.value() != null) {
                LOGGER.trace("Received event {} = '{}'", getString(record.key()), getString(record.value()));
                entries.put(record.key(), record.value());
            }
        }

        try {
            cache.putAll(entries);
        }
        catch (Exception e) {
            throw new DebeziumException(e);
        }

        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }

        committer.markBatchFinished();
    }
}
