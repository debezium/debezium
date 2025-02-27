/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.ConfigDefinitionMetadataTest;
import io.debezium.config.Configuration;

public class PostgresConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public PostgresConnectorConfigDefTest() {
        super(new YugabyteDBConnector());
    }

    @Test
    public void shouldSetReplicaAutoSetValidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1:FULL,testSchema_2.testTable_2:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetInvalidValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, "testSchema_1.testTable_1;FULL,testSchema_2.testTable_2;;DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }

    @Test
    public void shouldSetReplicaAutoSetRegExValue() {

        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, ".*.test.*:FULL,testSchema_2.*:DEFAULT");

        int problemCount = PostgresConnectorConfig.validateReplicaAutoSetField(
                configBuilder.build(), PostgresConnectorConfig.REPLICA_IDENTITY_AUTOSET_VALUES, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }

    @Test
    public void shouldValidateWithCorrectSingleHostnamePattern() {
        validateCorrectHostname(false);
    }

    @Test
    public void shouldValidateWithCorrectMultiHostnamePattern() {
        validateCorrectHostname(true);
    }

    @Test
    public void shouldFailWithInvalidCharacterInHostname() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "*invalidCharacter");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 1)).isTrue();
    }

    @Test
    public void shouldFailIfInvalidMultiHostFormatSpecified() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "127.0.0.1,127.0.0.2,127.0.0.3");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 1)).isTrue();
    }

    @Test
    public void shouldFailIfInvalidMultiHostFormatSpecifiedWithInvalidCharacter() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, "127.0.0.1,127.0.0.2,127.0.0.3+");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 2)).isTrue();
    }

    @Test
    public void shouldFailIfSlotRangesSpecifiedWithoutParallelStreamingMode() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STREAMING_MODE, PostgresConnectorConfig.StreamingMode.DEFAULT)
                .with(PostgresConnectorConfig.SLOT_RANGES, "0,10;10,65536");

        boolean valid = PostgresConnectorConfig.SLOT_RANGES.validate(
                configBuilder.build(), (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(valid).isFalse();
    }

    @Test
    public void ensureNoErrorWhenProperParallelStreamingConfigSpecified() {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.STREAMING_MODE, PostgresConnectorConfig.StreamingMode.PARALLEL)
                .with(PostgresConnectorConfig.SLOT_RANGES, "0,10;10,65536");

        boolean valid = PostgresConnectorConfig.SLOT_RANGES.validate(
                configBuilder.build(), (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat(valid).isTrue();
    }

    public void validateCorrectHostname(boolean multiNode) {
        Configuration.Builder configBuilder = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.HOSTNAME, multiNode ? "127.0.0.1:5433,127.0.0.2:5433,127.0.0.3:5433" : "127.0.0.1");

        int problemCount = PostgresConnectorConfig.validateYBHostname(
          configBuilder.build(), PostgresConnectorConfig.HOSTNAME, (field, value, problemMessage) -> System.out.println(problemMessage));

        assertThat((problemCount == 0)).isTrue();
    }
}
