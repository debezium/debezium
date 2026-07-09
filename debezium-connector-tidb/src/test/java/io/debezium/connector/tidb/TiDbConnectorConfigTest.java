/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * Unit tests for {@link TiDbConnectorConfig}.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectorConfigTest {

    private static Configuration.Builder minimalConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory");
    }

    @Test
    public void shouldValidateMinimalConfiguration() {
        final Configuration config = minimalConfig().build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, System.err::println)).isTrue();
    }

    @Test
    public void shouldNotValidateWithoutTicdcBootstrapServers() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory")
                .build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isFalse();
    }

    @Test
    public void shouldNotValidateWithoutTicdcTopics() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isFalse();
    }

    @Test
    public void shouldParseTopicList() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(
                minimalConfig().with(TiDbConnectorConfig.TICDC_TOPICS, "topic-a, topic-b ,topic-c").build());
        assertThat(config.getTicdcTopics()).containsExactly("topic-a", "topic-b", "topic-c");
    }

    @Test
    public void shouldDefaultToNoDataSnapshotModeAndEarliestOffset() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(minimalConfig().build());
        assertThat(config.getSnapshotMode()).isEqualTo(TiDbConnectorConfig.SnapshotMode.NO_DATA);
        assertThat(config.getTicdcInitialOffset()).isEqualTo(TiDbConnectorConfig.InitialOffset.EARLIEST);
    }

    @Test
    public void shouldPassThroughConsumerProperties() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(
                minimalConfig()
                        .with("ticdc.consumer.security.protocol", "SASL_SSL")
                        .with("ticdc.consumer.fetch.max.bytes", "1048576")
                        .build());

        assertThat(config.getTicdcConsumerProperties())
                .containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("fetch.max.bytes", "1048576");
    }

    @Test
    public void shouldFilterBuiltInDatabases() {
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("mysql")).isTrue();
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("INFORMATION_SCHEMA")).isTrue();
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("metrics_schema")).isTrue();
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("inventory")).isFalse();
    }
}
