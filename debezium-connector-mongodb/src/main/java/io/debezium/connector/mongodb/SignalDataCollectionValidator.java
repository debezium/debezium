/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import io.debezium.util.Strings;

/**
 * Validates {@code signal.data.collection}: collection existence and namespace shape. Unlike the relational
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
     * deployments). No-op unless enabled and the source channel is on.
     */
    public static SignalDataCollectionValidationResult validate(MongoClient client, SignalDataCollectionValidationRequest request) {
        List<String> errors = new ArrayList<>();
        if (request.validationEnabled() && request.sourceChannelEnabled()) {
            for (String rawValue : request.rawValues()) {
                if (Strings.isNullOrBlank(rawValue)) {
                    continue;
                }
                try {
                    checkSignalDataCollection(client, rawValue, errors);
                }
                catch (RuntimeException e) {
                    LOGGER.warn("{} Could not validate signal data collection '{}'", LOG_PREFIX, rawValue, e);
                }
            }
        }
        return new SignalDataCollectionValidationResult(errors);
    }

    private static void checkSignalDataCollection(MongoClient client, String rawValue, List<String> errors) {
        CollectionId collectionId = CollectionId.parse(rawValue);
        if (collectionId == null) {
            fail(errors, String.format("signal.data.collection must be specified as '<database>.<collection>', not '%s'.", rawValue));
            return;
        }

        // A nonexistent database yields an empty (not an error) collection list, so this also covers that case.
        List<String> collectionNames = new ArrayList<>();
        client.getDatabase(collectionId.dbName()).listCollectionNames().into(collectionNames);
        if (collectionNames.contains(collectionId.name())) {
            LOGGER.info("{} Signal data collection '{}' is valid.", LOG_PREFIX, rawValue);
        }
        else {
            fail(errors, String.format("Signal data collection '%s' does not exist.", rawValue));
        }
    }

    private static void fail(List<String> errors, String problem) {
        LOGGER.warn("{} {}", LOG_PREFIX, problem);
        errors.add(problem);
    }
}
