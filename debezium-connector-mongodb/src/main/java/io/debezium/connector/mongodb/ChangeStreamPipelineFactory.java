/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static java.util.function.Predicate.not;
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

    public static final String LIST_DELIMITER = ",";
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamPipelineFactory.class);

    private final MongoDbConnectorConfig connectorConfig;
    private final FilterConfig filterConfig;

    ChangeStreamPipelineFactory(MongoDbConnectorConfig connectorConfig, FilterConfig filterConfig) {
        this.connectorConfig = connectorConfig;
        this.filterConfig = filterConfig;
    }

    ChangeStreamPipeline create() {
        var sizePipeline = createSizePipeline();
        var splitPipeline = createSplitPipeline();
        var userAndInternalPipeline = createUserAndInternalPipeline();

        // Resolve and combine pipelines serially
        var effectivePipeline = sizePipeline
                .then(userAndInternalPipeline)
                .then(splitPipeline);

        LOGGER.info("Effective change stream pipeline: {}", effectivePipeline);
        return effectivePipeline;
    }

    private ChangeStreamPipeline createUserAndInternalPipeline() {
        var internalPipeline = createInternalPipeline();
        var userPipeline = createUserPipeline();

        switch (connectorConfig.getCursorPipelineOrder()) {
            case INTERNAL_FIRST:
                return internalPipeline.then(userPipeline);
            case USER_FIRST:
                return userPipeline.then(internalPipeline);
            case USER_ONLY:
                return userPipeline;
            default:
                // this should never happen
                throw new DebeziumException("Unknown aggregation pipeline order");
        }
    }

    private ChangeStreamPipeline createSizePipeline() {
        if (connectorConfig.getOversizeHandlingMode() != MongoDbConnectorConfig.OversizeHandlingMode.SKIP) {
            return new ChangeStreamPipeline();
        }
        var threshold = connectorConfig.getOversizeSkipThreshold();
        var fullDocument = expr(
                lte(bsonSize("$fullDocument"), threshold));
        var fullDocumentBeforeChange = expr(
                lte(bsonSize("$fullDocumentBeforeChange"), threshold));

        var stage = Aggregates.match(
                Filters.and(fullDocument, fullDocumentBeforeChange));

        return new ChangeStreamPipeline(stage);
    }

    private ChangeStreamPipeline createSplitPipeline() {
        if (connectorConfig.getOversizeHandlingMode() != MongoDbConnectorConfig.OversizeHandlingMode.SPLIT) {
            return new ChangeStreamPipeline();
        }

        return new ChangeStreamPipeline(splitLargeEvent());
    }

    private ChangeStreamPipeline createInternalPipeline() {
        // Resolve the leaf filters
        var filters = Stream
                .of(
                        createCollectionFilter(filterConfig),
                        createOperationTypeFilter(connectorConfig, filterConfig))
                .flatMap(Optional::stream)
                .collect(toList());

        // Combine
        var andFilter = Filters.and(filters);
        var matchFilter = Aggregates.match(andFilter);

        // Done if matching databases and collections as literals
        if (filterConfig.isLiteralsMatchMode()) {
            return new ChangeStreamPipeline(matchFilter);
        }

        // To match databases and collections as regexes we need the following transformations
        return createRegexMatchingInternalPipeline(matchFilter);
    }

    private ChangeStreamPipeline createRegexMatchingInternalPipeline(Bson matchFilter) {
        // TODO: $replaceRoot has performance impact. We should provide different implementations based on MongoDB version
        // Note that change streams cannot use indexes:
        // - https://www.mongodb.com/docs/manual/administration/change-streams-production-recommendations/#indexes-and-performance
        return new ChangeStreamPipeline(
                // Materialize a "namespace" field so that we can do qualified collection name matching with regexes per
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

    private static Optional<Bson> createDatabaseAndCollectionRegexFilters(FilterConfig filterConfig) {
        // Database filters
        // Note: No need to exclude `filterConfig.getBuiltInDbNames()` since these are not streamed per
        // https://www.mongodb.com/docs/manual/changeStreams/#watch-a-collection--database--or-deployment
        var dbFilters = Optional.<Bson> empty()
                .or(() -> filterConfig.getDbIncludeList()
                        .map(value -> Filters.regex("event.ns.db", value.replaceAll(",", "|"), "i")))
                .or(() -> filterConfig.getDbExcludeList()
                        .map(value -> Filters.regex("event.ns.db", "(?!" + value.replaceAll(",", "|") + ")", "i")));

        // Collection filters
        var collectionsFilters = Optional.<Bson> empty()
                .or(() -> filterConfig.getCollectionIncludeList()
                        .map(value -> Filters.regex("namespace", value.replaceAll(",", "|"), "i")))
                .or(() -> filterConfig.getCollectionExcludeList()
                        .map(value -> Filters.regex("namespace", "(?!" + value.replaceAll(",", "|") + ")", "i")));

        return andFilters(
                dbFilters,
                collectionsFilters);
    }

    private static Optional<Bson> createDatabaseAndCollectionLiteralFilters(FilterConfig filterConfig) {
        // Database filters
        // Note: No need to exclude `filterConfig.getBuiltInDbNames()` since these are not streamed per
        // https://www.mongodb.com/docs/manual/changeStreams/#watch-a-collection--database--or-deployment
        var dbFilters = Optional.<Bson> empty()
                .or(() -> filterConfig.getDbIncludeList()
                        .map(ChangeStreamPipelineFactory::splitList)
                        .map(dbs -> Filters.in("ns.db", dbs)))
                .or(() -> filterConfig.getDbExcludeList()
                        .map(ChangeStreamPipelineFactory::splitList)
                        .map(dbs -> Filters.nin("ns.db", dbs)));

        // Collection filters
        var collectionsFilters = Optional.<Bson> empty()
                .or(() -> filterConfig.getCollectionIncludeList()
                        .map(ChangeStreamPipelineFactory::splitNamespaceList)
                        .map(nss -> Filters.in("ns", nss)))
                .or(() -> filterConfig.getCollectionExcludeList()
                        .map(ChangeStreamPipelineFactory::splitNamespaceList)
                        .map(nss -> Filters.nin("ns", nss)));

        return andFilters(
                dbFilters,
                collectionsFilters);
    }

    private static Optional<Bson> createCollectionFilter(FilterConfig filterConfig) {
        var dbAndCollectionFilters = Optional.<Bson> empty();
        var includedSignalCollectionFilters = Optional.<Bson> empty();

        if (filterConfig.isLiteralsMatchMode()) {
            dbAndCollectionFilters = createDatabaseAndCollectionLiteralFilters(filterConfig);
            includedSignalCollectionFilters = filterConfig
                    .getSignalDataCollection()
                    .map(ChangeStreamPipelineFactory::namespaceBson)
                    .map(ns -> Filters.eq("ns", ns));
        }
        else {
            dbAndCollectionFilters = createDatabaseAndCollectionRegexFilters(filterConfig);
            includedSignalCollectionFilters = filterConfig
                    .getSignalDataCollection()
                    .map(col -> Filters.eq("namespace", col));
        }

        // Combined filters
        return orFilters(
                dbAndCollectionFilters,
                includedSignalCollectionFilters);
    }

    private static Optional<Bson> createOperationTypeFilter(MongoDbConnectorConfig connectorConfig, FilterConfig filterConfig) {
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

        // for regexes the original change event doc is placed under event field
        var field = filterConfig.isLiteralsMatchMode()
                ? "operationType"
                : "event.operationType";

        return Optional.of(Filters.in(field, includedOperations.stream()
                .map(OperationType::getValue)
                .collect(toList())));
    }

    @SafeVarargs
    private static Optional<Bson> andFilters(Optional<Bson>... filters) {
        var resolved = resolveFilters(filters);
        return andFilters(resolved);
    }

    private static Optional<Bson> andFilters(List<Bson> filters) {
        if (filters.isEmpty()) {
            return Optional.empty();
        }
        else if (filters.size() == 1) {
            return Optional.of(filters.get(0));
        }
        else {
            return Optional.of(Filters.and(filters));
        }
    }

    @SafeVarargs
    private static Optional<Bson> orFilters(Optional<Bson>... filters) {
        var resolved = resolveFilters(filters);
        return orFilters(resolved);
    }

    private static Optional<Bson> orFilters(List<Bson> filters) {
        if (filters.isEmpty()) {
            return Optional.empty();
        }
        else if (filters.size() == 1) {
            return Optional.of(filters.get(0));
        }
        else {
            return Optional.of(Filters.or(filters));
        }
    }

    @SafeVarargs
    private static List<Bson> resolveFilters(Optional<Bson>... filters) {
        return Stream.of(filters)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    private static Bson splitLargeEvent() {
        return new BasicDBObject("$changeStreamSplitLargeEvent", new BasicDBObject());
    }

    private static Bson concat(Object... expressions) {
        return new BasicDBObject("$concat", List.of(expressions));
    }

    private static Bson bsonSize(Object document) {
        return new BasicDBObject("$bsonSize", document);
    }

    private static Bson lte(Object expr1, Object expr2) {
        return new BasicDBObject("$lte", List.of(expr1, expr2));
    }

    private static Bson expr(Object expr) {
        return new BasicDBObject("$expr", expr);
    }

    private static Bson namespaceBson(String namespace) {
        var nsAndCol = namespace.trim().split("\\.", 2);
        return new BasicDBObject()
                .append("db", nsAndCol[0])
                .append("coll", nsAndCol[1]);
    }

    private static List<String> splitList(String input) {
        var parts = input.split(LIST_DELIMITER);

        return Stream.of(parts)
                .map(String::trim)
                .filter(not(String::isEmpty))
                .collect(toList());
    }

    private static List<Bson> splitNamespaceList(String input) {
        var parts = input.split(LIST_DELIMITER);

        return Stream.of(parts)
                .map(String::trim)
                .filter(not(String::isEmpty))
                .map(ChangeStreamPipelineFactory::namespaceBson)
                .collect(toList());
    }
}
