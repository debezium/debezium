/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;

import io.debezium.data.Envelope.Operation;
import io.debezium.util.Strings;

/**
 * Validates {@code signal.data.collection}: collection existence and namespace shape, and that
 * {@code skipped.operations} does not filter out signals. Unlike the relational
 * connectors, MongoDB collections have no fixed schema, so there is no equivalent of the effective-column-count
 * check.
 *
 * @author Debezium Authors
 */
public class SignalDataCollectionValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SignalDataCollectionValidator.class);
    private static final String LOG_PREFIX = "[signal.data.collection.validation]";

    private SignalDataCollectionValidator() {
    }

    /**
     * Validates every configured signal data collection (multiple may be configured for multi-partition
     * deployments). No-op unless enabled, streaming-capable, and the source channel is on.
     */
    public static SignalDataCollectionValidationResult validate(MongoClient client, SignalDataCollectionValidationRequest request) {
        List<String> errors = new ArrayList<>();
        if (request.validationEnabled() && request.streamingCapable() && request.sourceChannelEnabled()) {
            for (String rawValue : request.rawValues()) {
                if (Strings.isNullOrBlank(rawValue)) {
                    continue;
                }
                try {
                    checkSignalDataCollection(client, rawValue, request.skippedSignalOperations(), errors);
                }
                catch (RuntimeException e) {
                    LOGGER.warn("{} Could not validate signal data collection '{}'", LOG_PREFIX, rawValue, e);
                }
            }
        }
        return new SignalDataCollectionValidationResult(errors);
    }

    private static void checkSignalDataCollection(MongoClient client, String rawValue, Set<Operation> skippedSignalOperations, List<String> errors) {
        checkSkippedSignalOperations(rawValue, skippedSignalOperations, errors);

        CollectionId collectionId = CollectionId.parse(rawValue);
        if (collectionId == null) {
            errors.add(String.format("signal.data.collection must be specified as '<database>.<collection>', not '%s'.", rawValue));
            return;
        }

        // A nonexistent database yields an empty (not an error) collection list, so this also covers that case.
        // Views are excluded because they emit no change events.
        List<String> collectionNames = new ArrayList<>();
        client.getDatabase(collectionId.dbName()).listCollectionNames().filter(Filters.eq("type", "collection")).into(collectionNames);
        if (!collectionNames.contains(collectionId.name())) {
            errors.add(String.format("Signal data collection '%s' does not exist or is not visible to the connector user.", rawValue));
        }
    }

    /** Flags skipped operations dropped before signals are read: 'c' loses every signal, and 'd' under insert_delete leaves incremental snapshot windows open. */
    private static void checkSkippedSignalOperations(String rawValue, Set<Operation> skippedSignalOperations, List<String> errors) {
        if (skippedSignalOperations.contains(Operation.CREATE)) {
            errors.add(String.format("Signal data collection '%s' will never receive signals because skipped.operations includes 'c'.", rawValue));
        }
        if (skippedSignalOperations.contains(Operation.DELETE)) {
            errors.add(String.format("Signal data collection '%s' cannot close incremental snapshot windows because skipped.operations "
                    + "includes 'd', which the insert_delete watermarking strategy requires.", rawValue));
        }
    }
}
