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
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

import io.debezium.junit.logging.LogInterceptor;

/**
 * Unit tests for {@link SignalDataCollectionValidator}: the enablement/gating checks, namespace-shape and existence
 * validation, and the exception-swallowing guarantee. {@link MongoClient} is mocked so no live MongoDB deployment is
 * required.
 */
@ExtendWith(MockitoExtension.class)
public class SignalDataCollectionValidatorTest {

    private static final String RAW_VALUE = "inventory.debezium_signal";
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";

    @Mock
    private MongoClient client;

    private LogInterceptor logInterceptor;

    @BeforeEach
    public void beforeEach() {
        logInterceptor = new LogInterceptor(SignalDataCollectionValidator.class);
    }

    private SignalDataCollectionValidationRequest enabledRequest(String... rawValues) {
        return new SignalDataCollectionValidationRequest(Arrays.asList(rawValues), true, true);
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
                new SignalDataCollectionValidationRequest(List.of(RAW_VALUE), false, true));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldDoNothingWhenSourceChannelDisabled() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client,
                new SignalDataCollectionValidationRequest(List.of(RAW_VALUE), true, false));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldSkipBlankSignalDataCollectionValues() {
        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(null, " "));

        assertThat(result.isValid()).isTrue();
    }

    @Test
    public void shouldLogInfoAndNotFailWhenSignalDataCollectionIsValid() {
        stubDatabaseCollections("inventory", "debezium_signal", "customers");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE));

        assertThat(result.isValid()).isTrue();
        assertThat(logInterceptor.containsMessage(LOG_PREFIX + " Signal data collection '" + RAW_VALUE + "' is valid.")).isTrue();
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
    public void shouldValidateEveryConfiguredSignalDataCollection() {
        // Multi-partition deployments can configure more than one signal.data.collection - each must be checked
        // independently, and a problem in one must not prevent the other from being validated.
        String secondRawValue = "inventory.other_signal";
        stubDatabaseCollections("inventory", "debezium_signal");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE, secondRawValue));

        assertThat(result.errors()).containsExactly("Signal data collection '" + secondRawValue + "' does not exist.");
    }

    @Test
    public void shouldSwallowRuntimeExceptionAndContinueToNextValue() {
        // The two values resolve to different databases, so stubbing the first to throw cannot mask the second
        // value's own, independent probe.
        String secondRawValue = "other.other_signal";
        when(client.getDatabase("inventory")).thenThrow(new RuntimeException("connection reset"));
        stubDatabaseCollections("other", "other_signal");

        SignalDataCollectionValidationResult result = SignalDataCollectionValidator.validate(client, enabledRequest(RAW_VALUE, secondRawValue));

        assertThat(logInterceptor.containsWarnMessage("Could not validate signal data collection '" + RAW_VALUE + "'")).isTrue();
        assertThat(result.isValid()).isTrue();
    }
}
