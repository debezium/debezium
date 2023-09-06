/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.OperationType;

import io.debezium.DebeziumException;
import io.debezium.connector.mongodb.Filters.FilterConfig;
import io.debezium.data.Envelope;

/**
 * A factory to produce a MongoDB change stream pipeline expression.
 */
class ChangeStreamPipelineFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamPipelineFactory.class);

    private final ReplicaSetOffsetContext rsOffsetContext;
    private final MongoDbConnectorConfig connectorConfig;
    private final FilterConfig filterConfig;

    ChangeStreamPipelineFactory(ReplicaSetOffsetContext rsOffsetContext, MongoDbConnectorConfig connectorConfig, FilterConfig filterConfig) {
        this.rsOffsetContext = rsOffsetContext;
        this.connectorConfig = connectorConfig;
        this.filterConfig = filterConfig;
    }

    ChangeStreamPipeline create() {
        // Resolve and combine internal and user pipelines serially
        var effectivePipeline = mergeUserAndInternalPipeline();

        LOGGER.info("Effective change stream pipeline: {}", effectivePipeline);
        return effectivePipeline;
    }

    private ChangeStreamPipeline mergeUserAndInternalPipeline() {
        var internalPipeline = createInternalPipeline();
        var userPipeline = createUserPipeline();

        switch (connectorConfig.getCursorPipelineOrder()) {
            case INTERNAL_FIRST:
                return internalPipeline.then(userPipeline);
            case USER_FIRST:
                return userPipeline.then(internalPipeline);
            default:
                // this should never happen
                throw new DebeziumException("Unknown aggregation pipeline order");
        }
    }

    private ChangeStreamPipeline createInternalPipeline() {
        // Resolve the leaf filters
        var filters = Stream
                .of(
                        createCollectionFilter(filterConfig),
                        createOperationTypeFilter(connectorConfig))
                .flatMap(Optional::stream)
                .collect(toList());

        // Combine
        var andFilter = Filters.and(filters);
        var matchFilter = Aggregates.match(andFilter);

        // Pipeline
        // Note that change streams cannot use indexes:
        // - https://www.mongodb.com/docs/manual/administration/change-streams-production-recommendations/#indexes-and-performance
        // Note that `$addFields` must be used over `$set`/ `$unset` to support MongoDB 4.0 which doesn't support these operators:
        // - https://www.mongodb.com/docs/manual/changeStreams/#modify-change-stream-output
        return new ChangeStreamPipeline(
                // Materialize a "namespace" field so that we can do qualified collection name matching per
                // the configuration requirements
                // We can't use $addFields nor $set as there is no way to unset the filed for AWS DocumentDB
                // Note that per the docs, if `$ns` doesn't exist, `$concat` will return `null`
                Aggregates.replaceRoot(new BasicDBObject(Map.of(
                        "namespace", concat("$ns.db", ".", "$ns.coll"),
                        "event", "$$ROOT"))),
                // Filter the documents
                matchFilter,

                // This is required to prevent driver `ChangeStreamDocument` deserialization issues:
                Aggregates.replaceRoot("$event"));
    }

    private ChangeStreamPipeline createUserPipeline() {
        // Delegate to the configuration
        return filterConfig.getUserPipeline();
    }

    private static Optional<Bson> createCollectionFilter(FilterConfig filterConfig) {
        // Database filters
        // Note: No need to exclude `filterConfig.getBuiltInDbNames()` since these are not streamed per
        // https://www.mongodb.com/docs/manual/changeStreams/#watch-a-collection--database--or-deployment
        var dbFilters = Optional.<Bson> empty();
        if (filterConfig.getDbIncludeList() != null) {
            dbFilters = Optional.of(Filters.regex("event.ns.db", filterConfig.getDbIncludeList().replaceAll(",", "|"), "i"));
        }
        else if (filterConfig.getDbExcludeList() != null) {
            dbFilters = Optional.of(Filters.regex("event.ns.db", "(?!" + filterConfig.getDbExcludeList().replaceAll(",", "|") + ")", "i"));
        }

        // Collection filters
        var collectionsFilters = Optional.<Bson> empty();
        if (filterConfig.getCollectionIncludeList() != null) {
            collectionsFilters = Optional
                    .of(Filters.regex("namespace", filterConfig.getCollectionIncludeList().replaceAll(",", "|"), "i"));
        }
        else if (filterConfig.getCollectionExcludeList() != null) {
            collectionsFilters = Optional
                    .of(Filters.regex("namespace", "(?!" + filterConfig.getCollectionExcludeList().replaceAll(",", "|") + ")", "i"));
        }
        var includedSignalCollectionFilters = Optional.<Bson> empty();
        if (filterConfig.getSignalDataCollection() != null) {
            includedSignalCollectionFilters = Optional.of(Filters.eq("namespace", filterConfig.getSignalDataCollection()));
        }

        // Combined filters
        return orFilters(
                includedSignalCollectionFilters,
                andFilters(
                        dbFilters,
                        collectionsFilters));
    }

    private static Optional<Bson> createOperationTypeFilter(MongoDbConnectorConfig connectorConfig) {
        // Per https://debezium.io/documentation/reference/stable/connectors/mongodb.html#mongodb-property-skipped-operations
        // > The supported operations include:
        // > - 'c' for inserts/create
        // > - 'u' for updates/replace,
        // > - 'd' for deletes,
        // > - 't' for truncates, and
        // > - 'none' to not skip any operations.
        // > By default, 'truncate' operations are skipped (not emitted by this connector).
        // However, 'truncate' is not supported since it doesn't exist as a
        // [MongoDB change type](https://www.mongodb.com/docs/manual/reference/change-events/). Also note that
        // support for 'none' effectively implies 'c', 'u', 'd'

        // First, begin by including all the supported Debezium change events
        var includedOperations = new ArrayList<OperationType>();
        includedOperations.add(OperationType.INSERT);
        includedOperations.add(OperationType.UPDATE);
        includedOperations.add(OperationType.REPLACE);
        includedOperations.add(OperationType.DELETE);

        // Next, remove any implied by the configuration
        var skippedOperations = connectorConfig.getSkippedOperations();
        if (skippedOperations.contains(Envelope.Operation.CREATE)) {
            includedOperations.remove(OperationType.INSERT);
        }
        if (skippedOperations.contains(Envelope.Operation.UPDATE)) {
            includedOperations.remove(OperationType.UPDATE);
            includedOperations.remove(OperationType.REPLACE);
        }
        if (skippedOperations.contains(Envelope.Operation.DELETE)) {
            includedOperations.remove(OperationType.DELETE);
        }

        return Optional.of(Filters.in("event.operationType", includedOperations.stream()
                .map(OperationType::getValue)
                .collect(toList())));
    }

    @SafeVarargs
    private static Optional<Bson> andFilters(Optional<Bson>... filters) {
        var resolved = resolveFilters(filters);
        if (resolved.isEmpty()) {
            return Optional.empty();
        }
        else if (resolved.size() == 1) {
            return Optional.of(resolved.get(0));
        }
        else {
            return Optional.of(Filters.and(resolved));
        }
    }

    @SafeVarargs
    private static Optional<Bson> orFilters(Optional<Bson>... filters) {
        var resolved = resolveFilters(filters);
        if (resolved.isEmpty()) {
            return Optional.empty();
        }
        else if (resolved.size() == 1) {
            return Optional.of(resolved.get(0));
        }
        else {
            return Optional.of(Filters.or(resolved));
        }
    }

    @SafeVarargs
    private static List<Bson> resolveFilters(Optional<Bson>... filters) {
        return Stream.of(filters)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    private static Bson concat(Object... expressions) {
        return new BasicDBObject("$concat", List.of(expressions));
    }
}
