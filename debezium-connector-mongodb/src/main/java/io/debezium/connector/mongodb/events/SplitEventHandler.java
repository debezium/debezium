/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.changestream.ChangeStreamDocument;

import io.debezium.DebeziumException;

public class SplitEventHandler<TResult> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitEventHandler.class);

    final List<ChangeStreamDocument<TResult>> fragmentBuffer = new ArrayList<>(16);

    public Optional<ChangeStreamDocument<TResult>> handle(ChangeStreamDocument<TResult> event) {
        var split = event.getSplitEvent();

        if (split != null) {
            var currentFragment = split.getFragment();
            var totalFragments = split.getOf();
            LOGGER.trace("Change Stream event is a fragment: {} of {}", currentFragment, totalFragments);
            fragmentBuffer.add(event);

            // not the final fragment
            if (currentFragment != totalFragments) {
                return Optional.empty();
            }

            // reconstruct the event and clear the buffer
            var merged = mergeEventFragments(fragmentBuffer);
            fragmentBuffer.clear();
            return Optional.of(merged);
        }

        if (!fragmentBuffer.isEmpty()) {
            LOGGER.error("Expected event fragment but a new event arrived");
            throw new DebeziumException("Missing event fragment");
        }

        return Optional.of(event);
    }

    public boolean isEmpty() {
        return fragmentBuffer.isEmpty();
    }

    private static <TResult> ChangeStreamDocument<TResult> mergeEventFragments(List<ChangeStreamDocument<TResult>> events) {
        var operationTypeString = firstOrNull(events, ChangeStreamDocument::getOperationTypeString);
        var resumeToken = events.get(events.size() - 1).getResumeToken();
        var namespaceDocument = firstOrNull(events, ChangeStreamDocument::getNamespaceDocument);
        var destinationNamespaceDocument = firstOrNull(events, ChangeStreamDocument::getDestinationNamespaceDocument);
        var fullDocument = firstOrNull(events, ChangeStreamDocument::getFullDocument);
        var fullDocumentBeforeChange = firstOrNull(events, ChangeStreamDocument::getFullDocumentBeforeChange);
        var documentKey = firstOrNull(events, ChangeStreamDocument::getDocumentKey);
        var clusterTime = firstOrNull(events, ChangeStreamDocument::getClusterTime);
        var updateDescription = firstOrNull(events, ChangeStreamDocument::getUpdateDescription);
        var txnNumber = firstOrNull(events, ChangeStreamDocument::getTxnNumber);
        var lsid = firstOrNull(events, ChangeStreamDocument::getLsid);
        var wallTime = firstOrNull(events, ChangeStreamDocument::getWallTime);
        var extraElements = firstOrNull(events, ChangeStreamDocument::getExtraElements);

        return new ChangeStreamDocument<TResult>(
                operationTypeString,
                resumeToken,
                namespaceDocument,
                destinationNamespaceDocument,
                fullDocument,
                fullDocumentBeforeChange,
                documentKey,
                clusterTime,
                updateDescription,
                txnNumber,
                lsid,
                wallTime,
                null,
                extraElements);
    }

    private static <TResult, T> T firstOrNull(Collection<ChangeStreamDocument<TResult>> events, Function<ChangeStreamDocument<TResult>, T> getter) {
        return events.stream()
                .map(getter)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }
}
