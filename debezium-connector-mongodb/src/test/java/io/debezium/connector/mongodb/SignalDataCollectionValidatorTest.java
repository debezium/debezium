/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;

/**
 * Unit tests for {@link SignalDataCollectionValidator}: the enablement/gating checks, namespace-shape and existence
 * validation, and the exception-swallowing guarantee. {@link MongoClient} and {@link MongoDbConnectorConfig} are
 * mocked so no live MongoDB deployment is required.
 */
@ExtendWith(MockitoExtension.class)
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "inventory.debezium_signal";
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";

    @Mock
    private MongoClient client;

    @Mock
    private MongoDbConnectorConfig connectorConfig;

    private ConfigValue signalDataCollectionValue;
    private LogInterceptor logInterceptor;

    @BeforeEach
    public void beforeEach() {
        signalDataCollectionValue = new ConfigValue("signal.data.collection");
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);
    }

    /**
     * Stubs the two enablement gates open and configures the given signal data collection values, so the
     * validator's loop body actually runs. Tests for the gates themselves stub only what each gate reads.
     */
    private void stubEnabled(String... rawValues) {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of(SourceSignalChannel.CHANNEL_NAME));
        when(connectorConfig.getSignalingDataCollectionIds()).thenReturn(Arrays.asList(rawValues));
    }

    private void stubDatabaseCollections(String dbName, String... collectionNames) {
        ListCollectionNamesIterable iterable = mock(ListCollectionNamesIterable.class);
        doAnswer(invocation -> {
            Collection<String> target = invocation.getArgument(0);
            target.addAll(List.of(collectionNames));
            return target;
        }).when(iterable).into(any());

        MongoDatabase database = mock(MongoDatabase.class);
        when(database.listCollectionNames()).thenReturn(iterable);
        when(client.getDatabase(dbName)).thenReturn(database);
    }

    @Test
    public void shouldDoNothingWhenValidationDisabled() {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(false);

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() {
        when(connectorConfig.isSignalDataCollectionValidationEnabled()).thenReturn(true);
        when(connectorConfig.getEnabledChannels()).thenReturn(List.of("kafka"));

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldSkipBlankSignalDataCollectionValues() {
        stubEnabled(null, " ");

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }

    @Test
    public void shouldLogInfoAndNotFailWhenSignalDataCollectionIsValid() {
        stubEnabled(RAW_VALUE);
        stubDatabaseCollections("inventory", "debezium_signal", "customers");

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
        assertThat(logInterceptor.containsMessage(LOG_PREFIX + " Signal data collection '" + RAW_VALUE + "' is valid.")).isTrue();
    }

    @Test
    public void shouldFailWhenCollectionDoesNotExist() {
        stubEnabled(RAW_VALUE);
        stubDatabaseCollections("inventory", "customers");

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("Signal data collection '" + RAW_VALUE + "' does not exist.");
    }

    @Test
    public void shouldFailWhenDatabaseDoesNotExist() {
        // A nonexistent database yields an empty (not an error) collection list from the driver, so it must be
        // reported the same way as a missing collection rather than skipped or treated as a probe failure.
        stubEnabled(RAW_VALUE);
        stubDatabaseCollections("inventory");

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("Signal data collection '" + RAW_VALUE + "' does not exist.");
    }

    @Test
    public void shouldFailWhenValueIsNotInDatabaseDotCollectionShape() {
        stubEnabled("debezium_signal");

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages()).containsExactly(
                "signal.data.collection must be specified as '<database>.<collection>', not 'debezium_signal'.");
    }

    @Test
    public void shouldValidateEveryConfiguredSignalDataCollection() {
        // Multi-partition deployments can configure more than one signal.data.collection - each must be checked
        // independently, and a problem in one must not prevent the other from being validated.
        String secondRawValue = "inventory.other_signal";
        stubEnabled(RAW_VALUE, secondRawValue);
        stubDatabaseCollections("inventory", "debezium_signal");

        SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue);

        assertThat(signalDataCollectionValue.errorMessages())
                .containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowRuntimeExceptionAndContinueToNextValue() {
        // The two values resolve to different databases, so stubbing the first to throw cannot mask the second
        // value's own, independent probe.
        String secondRawValue = "other.other_signal";
        stubEnabled(RAW_VALUE, secondRawValue);
        when(client.getDatabase("inventory")).thenThrow(new RuntimeException("connection reset"));
        stubDatabaseCollections("other", "other_signal");

        assertThatCode(() -> SignalDataCollectionValidator.validate(client, connectorConfig, signalDataCollectionValue))
                .doesNotThrowAnyException();

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(signalDataCollectionValue.errorMessages()).isEmpty();
    }
}
