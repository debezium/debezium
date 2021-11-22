/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Set;
import java.util.function.Predicate;

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
    private final Predicate<CollectionId> collectionFilter;
    private final FieldSelector fieldSelector;

    /**
     * Create an instance of the filters.
     *
     * @param config the configuration; may not be null
     */
    public Filters(Configuration config) {
        String dbIncludeList = config.getFallbackStringProperty(MongoDbConnectorConfig.DATABASE_INCLUDE_LIST, MongoDbConnectorConfig.DATABASE_WHITELIST);
        String dbExcludeList = config.getFallbackStringProperty(MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST, MongoDbConnectorConfig.DATABASE_BLACKLIST);
        if (dbIncludeList != null && !dbIncludeList.trim().isEmpty()) {
            databaseFilter = Predicates.includes(dbIncludeList);
        }
        else if (dbExcludeList != null && !dbExcludeList.trim().isEmpty()) {
            databaseFilter = Predicates.excludes(dbExcludeList);
        }
        else {
            databaseFilter = (db) -> true;
        }

        String collectionIncludeList = config.getFallbackStringProperty(MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST,
                MongoDbConnectorConfig.COLLECTION_WHITELIST);
        String collectionExcludeList = config.getFallbackStringProperty(MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST,
                MongoDbConnectorConfig.COLLECTION_BLACKLIST);
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
    }

    /**
     * Get the predicate function that determines whether the given database is to be included.
     *
     * @return the database filter; never null
     */
    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }

    /**
     * Get the predicate function that determines whether the given collection is to be included.
     *
     * @return the collection filter; never null
     */
    public Predicate<CollectionId> collectionFilter() {
        return collectionFilter;
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
}
