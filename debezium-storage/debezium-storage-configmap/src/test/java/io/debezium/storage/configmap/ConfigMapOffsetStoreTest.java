/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.configmap;

import static io.debezium.storage.configmap.ConfigMapOffsetStore.OFFSET_STORAGE_CONFIGMAP_NAME_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.debezium.embedded.EmbeddedWorkerConfig;
import io.fabric8.kubeapitest.junit.EnableKubeAPIServer;
import io.fabric8.kubeapitest.junit.KubeConfig;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

@EnableKubeAPIServer
public class ConfigMapOffsetStoreTest {

    public static final String MY_OFFSETS_MAP = "my-offsets-map";

    // static is required in case the config is shared between test cases
    @KubeConfig
    static String kubeConfigYaml;
    private KubernetesClient client;

    @AfterEach
    void tearDown() {
        client.configMaps().withName(MY_OFFSETS_MAP).delete();
    }

    @Test
    void whenConfiguredConfigMapIsNotPresentItMustBeCreated() {

        ConfigMapOffsetStore offsetStore = new ConfigMapOffsetStore(kubeConfigYaml);
        Map<String, String> embeddedConfig = new HashMap<>();
        embeddedConfig.put(OFFSET_STORAGE_CONFIGMAP_NAME_CONFIG, MY_OFFSETS_MAP);
        offsetStore.configure(new EmbeddedWorkerConfig(embeddedConfig));
        offsetStore.start();

        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(kubeConfigYaml))
                .build();

        var cm = client.configMaps().withName(MY_OFFSETS_MAP);

        assertThat(cm).isNotNull();
        assertThat(cm.get().getData()).isEmpty();
    }

    @Test
    void whenTheSetMethodIsCalledTheConfigMapWillBeUpdatedWithTheCurrentOffset() throws ExecutionException, InterruptedException {

        ConfigMapOffsetStore offsetStore = new ConfigMapOffsetStore(kubeConfigYaml);
        Map<String, String> embeddedConfig = new HashMap<>();
        embeddedConfig.put(OFFSET_STORAGE_CONFIGMAP_NAME_CONFIG, MY_OFFSETS_MAP);
        offsetStore.configure(new EmbeddedWorkerConfig(embeddedConfig));
        offsetStore.start();

        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(kubeConfigYaml))
                .build();

        var cm = client.configMaps().withName(MY_OFFSETS_MAP);

        assertThat(cm).isNotNull();

        offsetStore.set(
                Map.of(offsetStore.toByteBuffer("[\"kafka\",{\"server\":\"inventory\", \"database\":\"db1\"}]"),
                        offsetStore.toByteBuffer("{\"last_snapshot_record\":true,\"lsn\":37929376,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":true}")),
                (error, result) -> {
                }).get();
        assertThat(client.configMaps().withName(MY_OFFSETS_MAP).get().getBinaryData()).containsExactly(entry("kafka.server-inventory_database-db1",
                "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6dHJ1ZSwibHNuIjozNzkyOTM3NiwidHhJZCI6Nzc1LCJ0c191c2VjIjoxNzMwMTEwNjEzNzY2Njc0LCJzbmFwc2hvdCI6dHJ1ZX0="));

        offsetStore.set(
                Map.of(offsetStore.toByteBuffer("[\"kafka\",{\"server\":\"inventory\", \"database\":\"db1\"}]"),
                        offsetStore.toByteBuffer("{\"last_snapshot_record\":false,\"lsn\":37929376,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":true}")),
                (error, result) -> {
                }).get();
        assertThat(cm.get().getBinaryData()).containsExactly(entry("kafka.server-inventory_database-db1",
                "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6ZmFsc2UsImxzbiI6Mzc5MjkzNzYsInR4SWQiOjc3NSwidHNfdXNlYyI6MTczMDExMDYxMzc2NjY3NCwic25hcHNob3QiOnRydWV9"));

        offsetStore.set(Map.of(
                offsetStore.toByteBuffer("[\"kafka\",{\"server\":\"inventory\", \"database\":\"db1\"}]"),
                offsetStore.toByteBuffer("{\"last_snapshot_record\":false,\"lsn\":37929376,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":false}"),
                offsetStore.toByteBuffer("[\"kafka\",{\"server\":\"myserver\", \"database\":\"db1\"}]"),
                offsetStore.toByteBuffer("{\"last_snapshot_record\":false,\"lsn\":100,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":false}")),
                (error, result) -> {
                }).get();
        assertThat(cm.get().getBinaryData()).containsOnly(
                entry("kafka.server-inventory_database-db1",
                        "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6ZmFsc2UsImxzbiI6Mzc5MjkzNzYsInR4SWQiOjc3NSwidHNfdXNlYyI6MTczMDExMDYxMzc2NjY3NCwic25hcHNob3QiOmZhbHNlfQ=="),
                entry("kafka.server-myserver_database-db1",
                        "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6ZmFsc2UsImxzbiI6MTAwLCJ0eElkIjo3NzUsInRzX3VzZWMiOjE3MzAxMTA2MTM3NjY2NzQsInNuYXBzaG90IjpmYWxzZX0="));

    }

    @Test
    void whenAnOffsetIsPresentItWillBeLoadedCorrectly() throws ExecutionException, InterruptedException {

        ConfigMapOffsetStore offsetStore = new ConfigMapOffsetStore(kubeConfigYaml);
        Map<String, String> embeddedConfig = new HashMap<>();
        embeddedConfig.put(OFFSET_STORAGE_CONFIGMAP_NAME_CONFIG, MY_OFFSETS_MAP);
        offsetStore.configure(new EmbeddedWorkerConfig(embeddedConfig));

        client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(kubeConfigYaml))
                .build();

        client.configMaps().resource(new ConfigMapBuilder()
                .withNewMetadata()
                .withName(MY_OFFSETS_MAP)
                .endMetadata()
                .withBinaryData(Map.of(
                        "kafka.server-inventory_database-db1",
                        "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6ZmFsc2UsImxzbiI6Mzc5MjkzNzYsInR4SWQiOjc3NSwidHNfdXNlYyI6MTczMDExMDYxMzc2NjY3NCwic25hcHNob3QiOmZhbHNlfQ==",
                        "kafka.server-myserver_database-db1",
                        "eyJsYXN0X3NuYXBzaG90X3JlY29yZCI6ZmFsc2UsImxzbiI6MTAwLCJ0eElkIjo3NzUsInRzX3VzZWMiOjE3MzAxMTA2MTM3NjY2NzQsInNuYXBzaG90IjpmYWxzZX0="))
                .build())
                .create();

        offsetStore.start();

        Map<String, String> data = offsetStore.get(Arrays.asList(
                offsetStore.toByteBuffer("[\"kafka\",{\"server\":\"inventory\",\"database\":\"db1\"}]"),
                offsetStore.toByteBuffer("not_existing"),
                offsetStore.toByteBuffer("[\"kafka\",{\"server\":\"myserver\",\"database\":\"db1\"}]"))).get()
                .entrySet().stream()
                .filter(e -> offsetStore.fromByteBuffer(e.getValue()) != null)
                .collect(Collectors.toMap(
                        e -> offsetStore.fromByteBuffer(e.getKey()),
                        e -> offsetStore.fromByteBuffer(e.getValue())));

        assertThat(data).containsExactly(
                entry("[\"kafka\",{\"server\":\"inventory\",\"database\":\"db1\"}]",
                        "{\"last_snapshot_record\":false,\"lsn\":37929376,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":false}"),
                entry("[\"kafka\",{\"server\":\"myserver\",\"database\":\"db1\"}]",
                        "{\"last_snapshot_record\":false,\"lsn\":100,\"txId\":775,\"ts_usec\":1730110613766674,\"snapshot\":false}"));
    }
}
