/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.configmap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.spi.storage.DefaultOffsetStorageReader;
import io.debezium.spi.storage.DefaultOffsetStorageWriter;
import io.debezium.spi.storage.OffsetStorageReader;
import io.debezium.spi.storage.OffsetStorageWriter;
import io.debezium.spi.storage.OffsetStore;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

@Incubating
public class ConfigMapOffsetStore implements OffsetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMapOffsetStore.class);
    public static final String OFFSET_STORAGE_CONFIGMAP_NAME_CONFIG = "offset.storage.configmap.name";

    private String configMapName;

    private final ExecutorService executor = Executors.newFixedThreadPool(1, r -> {
        Thread t = new Thread(r, ConfigMapOffsetStore.class.getSimpleName());
        t.setDaemon(false);
        return t;
    });
    private final KubernetesClient k8sClient;
    private final ConfigMapFormatter configMapFormatter;

    private final Map<ByteBuffer, ByteBuffer> data = new ConcurrentHashMap<>();
    private ConfigMap configMap;

    public ConfigMapOffsetStore() { // Do not remove. This is used by the engine to instantiate the class.

        // Uses the auto mounted service account https://github.com/fabric8io/kubernetes-client/blob/main/doc/FAQ.md#running-kubernetesclient-from-within-a-pod-in-kubernetes-cluster
        k8sClient = new KubernetesClientBuilder().build();
        configMapFormatter = new ConfigMapFormatter();
    }

    public ConfigMapOffsetStore(String clientConfig) {
        k8sClient = new KubernetesClientBuilder().withConfig(Config.fromKubeconfig(clientConfig)).build();
        configMapFormatter = new ConfigMapFormatter();
    }

    @Override
    public void configure(Configuration configuration) {

        configMapName = configuration.getString(OFFSET_STORAGE_CONFIGMAP_NAME_CONFIG);

    }

    @Override
    public void start() {

        LOGGER.info("Starting ConfigMapOffsetStore with config map {}", this.configMapName);

        String currentNamespace = k8sClient.getConfiguration().getNamespace();

        LOGGER.debug("Trying to get config map {} from namespace {}", configMapName, currentNamespace);

        getOrCreateConfigMap(currentNamespace);

        load();
    }

    private void getOrCreateConfigMap(String currentNamespace) {

        try {
            configMap = k8sClient.configMaps()
                    .inNamespace(currentNamespace)
                    .withName(configMapName).get();

            if (configMap == null) {

                k8sClient.configMaps()
                        .inNamespace(currentNamespace)
                        .resource(new ConfigMapBuilder()
                                .withNewMetadata().withName(configMapName).endMetadata()
                                .build())
                        .create();

                configMap = k8sClient.configMaps()
                        .inNamespace(currentNamespace)
                        .withName(configMapName).get();
            }
        }
        catch (Exception e) {
            LOGGER.error("Error while get/create config map {}", configMapName, e);
            throw new DebeziumException(String.format("Error while get/create config map: %s", configMap), e);
        }
    }

    @Override
    public void stop() {

        executor.shutdown();
        k8sClient.close();
        LOGGER.info("Stopped ConfigMapOffsetStore");
    }

    private void load() {

        try {
            this.data.putAll(configMapFormatter.convertFromStorableFormat(configMap.getBinaryData()));
            LOGGER.info("Config map {} correctly loaded", configMap);
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unable to load data from config map: %s", configMapName), e);
        }
    }

    private void save() {

        try {

            k8sClient.configMaps()
                    .withName(configMapName)
                    .edit(cm -> new ConfigMapBuilder(cm)
                            .addToBinaryData(configMapFormatter.convertToStorableFormat(this.data)).build());

            LOGGER.debug("Offsets correctly stored into {} config map", configMap);
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unable to edit config map: %s", configMapName), e);
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {

        return executor.submit(() -> {
            Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
            for (ByteBuffer key : keys) {
                result.put(key, data.get(key));
            }
            return result;
        });
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, OffsetStore.Callback<Void> callback) {

        return executor.submit(() -> {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                LOGGER.debug("Storing offset with key {} and value {}", fromByteBuffer(entry.getKey()), fromByteBuffer(entry.getValue()));
                data.put(entry.getKey(), entry.getValue());
            }
            save();
            if (callback != null) {
                callback.onCompletion(null, null);
            }
            return null;
        });
    }

    public String fromByteBuffer(ByteBuffer data) {
        return (data != null) ? String.valueOf(StandardCharsets.UTF_8.decode(data.asReadOnlyBuffer())) : null;
    }

    public ByteBuffer toByteBuffer(String data) {
        return (data != null) ? ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)) : null;
    }

    @Override
    public OffsetStorageReader createReader(String namespace) {
        return new DefaultOffsetStorageReader(this, namespace);
    }

    @Override
    public OffsetStorageWriter createWriter(String namespace) {
        return new DefaultOffsetStorageWriter(this, namespace);
    }
}
