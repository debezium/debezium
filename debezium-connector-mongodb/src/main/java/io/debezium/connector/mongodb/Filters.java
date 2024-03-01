/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static io.debezium.config.CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.FieldSelector.FieldFilter;
import io.debezium.function.Predicates;
import io.debezium.util.Collect;

/**
 * A utility that is contains various filters for acceptable database names, {@link CollectionId}s, and fields.
 *
 * @author Randall Hauch
 */
public final class Filters {

    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("local", "admin", "config");
    private final Predicate<String> databaseFilter;
    private final String databaseIncludeList;
    private final String databaseExcludeList;

    private final Predicate<CollectionId> collectionFilter;
    private final String collectionIncludeList;
    private final String collectionExcludeList;
    private final FieldSelector fieldSelector;

    private final CommonConnectorConfig.Version version;

    private final MongoDbConnectorConfig.FiltersMatchMode filtersMatchMode;
    private final boolean literalMatchMode;
    private final boolean noneMatchMode;

    /**
     * Create an instance of the filters.
     *
     * @param config the configuration; may not be null
     */
    public Filters(Configuration config) {
        version = CommonConnectorConfig.Version.parse(config.getString(SOURCE_STRUCT_MAKER_VERSION));

        databaseIncludeList = normalize(config.getFallbackStringProperty(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, MongoDbConnectorConfig.DATABASE_WHITELIST));
        databaseExcludeList = normalize(config.getFallbackStringProperty(MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST, MongoDbConnectorConfig.DATABASE_BLACKLIST));
        if (databaseIncludeList != null && !databaseIncludeList.trim().isEmpty()) {
            databaseFilter = Predicates.includes(databaseIncludeList);
        }
        else if (databaseExcludeList != null && !databaseExcludeList.trim().isEmpty()) {
            databaseFilter = Predicates.excludes(databaseExcludeList);
        }
        else {
            databaseFilter = (db) -> true;
        }

        collectionIncludeList = normalize(config.getFallbackStringProperty(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST,
                MongoDbConnectorConfig.COLLECTION_WHITELIST));
        collectionExcludeList = normalize(config.getFallbackStringProperty(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST,
                MongoDbConnectorConfig.COLLECTION_BLACKLIST));
        final Predicate<CollectionId> collectionFilter;
        if (collectionIncludeList != null && !collectionIncludeList.trim().isEmpty()) {
            collectionFilter = Predicates.includes(collectionIncludeList, CollectionId::namespace);
        }
        else if (collectionExcludeList != null && !collectionExcludeList.trim().isEmpty()) {
            collectionFilter = Predicates.excludes(collectionExcludeList, CollectionId::namespace);
        }
        else {
            collectionFilter = (id) -> true;
        }
        Predicate<CollectionId> isNotBuiltIn = this::isNotBuiltIn;
        this.collectionFilter = isNotBuiltIn.and(collectionFilter);

        // Define the field selector that provides the field filter to exclude or rename fields in a document ...
        fieldSelector = FieldSelector.builder()
                .excludeFields(config.getFallbackStringProperty(MongoDbConnectorConfig.FIELD_EXCLUDE_LIST, MongoDbConnectorConfig.FIELD_BLACKLIST))
                .renameFields(config.getString(MongoDbConnectorConfig.FIELD_RENAMES))
                .build();

        String filterMatchModeValue = config.getString(MongoDbConnectorConfig.FILTERS_MATCH_MODE);
        this.filtersMatchMode = MongoDbConnectorConfig.FiltersMatchMode.parse(filterMatchModeValue, MongoDbConnectorConfig.FILTERS_MATCH_MODE.defaultValueAsString());
        this.literalMatchMode = MongoDbConnectorConfig.FiltersMatchMode.LITERAL.equals(filtersMatchMode);
        this.noneMatchMode = MongoDbConnectorConfig.FiltersMatchMode.NONE.equals(filtersMatchMode);
    }

    /**
     * Get the predicate function that determines whether the given database is to be included.
     *
     * @return the database filter; never null
     */
    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }

    public Optional<String> getDatabaseIncludeList() {
        return Optional.ofNullable(databaseIncludeList);
    }

    public boolean supportPipelineFilter() {
        return this.version == CommonConnectorConfig.Version.V2;
    }

    /**
     * Get the predicate function that determines whether the given collection is to be included.
     *
     * @return the collection filter; never null
     */
    public Predicate<CollectionId> collectionFilter() {
        return collectionFilter;
    }

    public Optional<String> getCollectionIncludeList() {
        return Optional.ofNullable(collectionIncludeList);
    }

    public boolean isLiteralsMatchMode() {
        return literalMatchMode;
    }

    public boolean isNoneMatchMode() {
        return noneMatchMode;
    }

    /**
     * Get the field filter for a given collection identifier.
     *
     * @param id the collection identifier, never null
     * @return the field filter; never null
     */
    public FieldFilter fieldFilterFor(CollectionId id) {
        return fieldSelector.fieldFilterFor(id);
    }

    protected boolean isNotBuiltIn(CollectionId id) {
        return !BUILT_IN_DB_NAMES.contains(id.dbName());
    }

    private static String normalize(String text) {
        if (text == null) {
            return null;
        }

        text = text.trim();
        if (text.isEmpty()) {
            return null;
        }

        return text;
    }

    private static Bson namespaceBson(String namespace) {
        String[] nsAndCol = namespace.trim().split("\\.", 2);
        return new BasicDBObject()
                .append("db", nsAndCol[0])
                .append("coll", nsAndCol[1]);
    }

    public static final String LIST_DELIMITER = ",";

    public static List<String> SplitList(String input) {
        String[] parts = input.split(LIST_DELIMITER);

        return Stream.of(parts)
                .map(String::trim)
                .filter(not(String::isEmpty))
                .collect(toList());
    }

    public static List<Bson> SplitNamespaceList(String input) {
        String[] parts = input.split(LIST_DELIMITER);

        return Stream.of(parts)
                .map(String::trim)
                .filter(not(String::isEmpty))
                .map(Filters::namespaceBson)
                .collect(toList());
    }

    static <T> Predicate<T> not(Predicate<? super T> target) {
        Objects.requireNonNull(target);
        return (Predicate<T>) target.negate();
    }
}
