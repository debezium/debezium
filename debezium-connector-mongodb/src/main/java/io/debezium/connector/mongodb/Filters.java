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

    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("local", "admin");

    private final Predicate<String> databaseFilter;
    private final Predicate<CollectionId> collectionFilter;
    private final FieldSelector fieldSelector;

    /**
     * Create an instance of the filters.
     * 
     * @param config the configuration; may not be null
     */
    public Filters(Configuration config) {
        String dbWhitelist = config.getString(MongoDbConnectorConfig.DATABASE_WHITELIST);
        String dbBlacklist = config.getString(MongoDbConnectorConfig.DATABASE_BLACKLIST);
        if (dbWhitelist != null && !dbWhitelist.trim().isEmpty()) {
            databaseFilter = Predicates.includes(dbWhitelist);
         } else if (dbBlacklist != null && !dbBlacklist.trim().isEmpty()) {
            databaseFilter = Predicates.excludes(dbBlacklist);
        } else {
            databaseFilter = (db)->true;
        }

        String collectionWhitelist = config.getString(MongoDbConnectorConfig.COLLECTION_WHITELIST);
        String collectionBlacklist = config.getString(MongoDbConnectorConfig.COLLECTION_BLACKLIST);
        final Predicate<CollectionId> collectionFilter;
        if (collectionWhitelist != null && !collectionWhitelist.trim().isEmpty()) {
            collectionFilter = Predicates.includes(collectionWhitelist, CollectionId::namespace);
        } else if (collectionBlacklist != null && !collectionBlacklist.trim().isEmpty()) {
            collectionFilter = Predicates.excludes(collectionBlacklist, CollectionId::namespace);
        } else {
            collectionFilter = (id) -> true;
        }
        Predicate<CollectionId> isNotBuiltIn = this::isNotBuiltIn;
        this.collectionFilter = isNotBuiltIn.and(collectionFilter);

        // Define the field selector that provides the field filter to exclude or rename fields in a document ...
        fieldSelector = FieldSelector.builder()
                .excludeFields(config.getString(MongoDbConnectorConfig.FIELD_BLACKLIST))
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
