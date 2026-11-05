/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import io.debezium.data.Envelope.Operation;
import io.debezium.junit.logging.LogInterceptor;

/**
 * Unit tests for {@link SignalDataCollectionValidator}: the enablement/gating checks, skipped signal operations,
 * namespace-shape and existence validation, and the exception-swallowing guarantee. {@link MongoClient} is mocked so no live MongoDB deployment is
 * required.
 */
@ExtendWith(MockitoExtension.class)
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "inventory.debezium_signal";

    @Mock
    private MongoClient client;

    private LogInterceptor logInterceptor;

    @BeforeEach
    public void beforeEach() {
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);
    }

    private SignalDataCollectionValidationRequest enabledRequest(String... rawValues) {
        return new SignalDataCollectionValidationRequest(Arrays.asList(rawValues), true, true, true, EnumSet.noneOf(Operation.class));
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
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client,
                new SignalDataCollectionValidationRequest(List.of(RAW_VALUE), false, true, true, EnumSet.noneOf(Operation.class)));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenNotStreamingCapable() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client,
                new SignalDataCollectionValidationRequest(List.of(RAW_VALUE), true, false, true, EnumSet.noneOf(Operation.class)));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client,
                new SignalDataCollectionValidationRequest(List.of(RAW_VALUE), true, true, false, EnumSet.noneOf(Operation.class)));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldSkipBlankSignalDataCollectionValues() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(null, " "));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldNotFailWhenSignalDataCollectionIsValid() {
        stubDatabaseCollections("inventory", "debezium_signal", "customers");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldFailWhenCollectionDoesNotExist() {
        stubDatabaseCollections("inventory", "customers");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE + "' does not exist.");
    }

    @Test
    public void shouldFailWhenValueIsNotInDatabaseDotCollectionShape() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest("debezium_signal"));

        assertThat(result.errors()).containsExactly("signal.data.collection must be specified as '<database>.<collection>', not 'debezium_signal'.");
    }

    @Test
    public void shouldFailWhenSkippedOperationsDropSignals() {
        // The change stream pipeline filters skipped operations server-side, so signal inserts never reach the connector.
        stubDatabaseCollections("inventory", "debezium_signal");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client,
                new SignalDataCollectionValidationRequest(List.of(RAW_VALUE), true, true, true, EnumSet.of(Operation.CREATE)));

        assertThat(result.errors()).containsExactly("Signal data collection '" + RAW_VALUE
                + "' will never receive signals because skipped.operations includes 'c', which the connector filters out before reading signals.");
    }

    @Test
    public void shouldValidateEveryConfiguredSignalDataCollection() {
        // Each configured signal.data.collection is checked independently; a problem in one must not skip the other.
        String secondRawValue = "inventory.other_signal";
        stubDatabaseCollections("inventory", "debezium_signal");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE, secondRawValue));

        assertThat(result.errors()).containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowRuntimeExceptionAndContinueToNextValue() {
        // The two values resolve to different databases, so stubbing the first to throw cannot mask the second's probe.
        String secondRawValue = "other.other_signal";
        when(client.getDatabase("inventory")).thenThrow(new RuntimeException("connection reset"));
        stubDatabaseCollections("other", "other_signal");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE, secondRawValue));

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(result.isValid()).isTrue();
    }
}
