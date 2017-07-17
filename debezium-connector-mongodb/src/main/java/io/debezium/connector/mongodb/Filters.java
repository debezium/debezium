/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Set;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.function.Predicates;
import io.debezium.util.Collect;

/**
 * A utility that is contains filters for acceptable collections.
 * 
 * @author Randall Hauch
 */
public final class Filters {

    protected static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("local", "admin");

    private final Predicate<CollectionId> collectionFilter;
    private final Predicate<String> databaseFilter;

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

        String whitelist = config.getString(MongoDbConnectorConfig.COLLECTION_WHITELIST);
        String blacklist = config.getString(MongoDbConnectorConfig.COLLECTION_BLACKLIST);
        Predicate<CollectionId> collectionFilter = null;
        if (whitelist != null && !whitelist.trim().isEmpty()) {
            collectionFilter = Predicates.includes(whitelist, CollectionId::namespace);
        } else if (blacklist != null && !blacklist.trim().isEmpty()) {
            collectionFilter = Predicates.excludes(blacklist, CollectionId::namespace);
        } else {
            collectionFilter = (id) -> true;
        }
        Predicate<CollectionId> isNotBuiltIn = this::isNotBuiltIn;
        this.collectionFilter = isNotBuiltIn.and(collectionFilter);
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
    
    protected boolean isNotBuiltIn(CollectionId id) {
        return !BUILT_IN_DB_NAMES.contains(id.dbName());
    }
}
