/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Set;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.FieldSelector.FieldFilter;
import io.debezium.function.Predicates;
import io.debezium.util.Collect;

/**
 * A utility that is contains various filters for acceptable database names, {@link CollectionId}s, and fields.
 *
 * @author Randall Hauch
 */
public final class Filters {

    private final FilterConfig config;
    private final Predicate<String> databaseFilter;
    private final Predicate<CollectionId> collectionFilter;
    private final FieldSelector fieldSelector;

    /**
     * Create an instance of the filters.
     *
     * @param configuration the configuration; may not be null
     */
    public Filters(Configuration configuration) {
        this.config = new FilterConfig(configuration);

        String dbIncludeList = config.getDbIncludeList();
        String dbExcludeList = config.getDbExcludeList();
        if (dbIncludeList != null) {
            databaseFilter = Predicates.includes(dbIncludeList);
        }
        else if (dbExcludeList != null) {
            databaseFilter = Predicates.excludes(dbExcludeList);
        }
        else {
            databaseFilter = (db) -> true;
        }

        String collectionIncludeList = config.getCollectionIncludeList();
        String collectionExcludeList = config.getCollectionExcludeList();
        final Predicate<CollectionId> collectionFilter;
        if (collectionIncludeList != null) {
            collectionFilter = Predicates.includes(collectionIncludeList, CollectionId::namespace);
        }
        else if (collectionExcludeList != null) {
            collectionFilter = Predicates.excludes(collectionExcludeList, CollectionId::namespace);
        }
        else {
            collectionFilter = (id) -> true;
        }

        Predicate<CollectionId> isNotBuiltIn = this::isNotBuiltIn;
        Predicate<CollectionId> finalCollectionFilter = isNotBuiltIn.and(collectionFilter);
        String signalDataCollection = config.getSignalDataCollection();
        if (signalDataCollection != null) {
            CollectionId signalDataCollectionId = CollectionId.parse("UNUSED", signalDataCollection);
            if (!finalCollectionFilter.test(signalDataCollectionId)) {
                final Predicate<CollectionId> signalDataCollectionPredicate = Predicates.includes(signalDataCollectionId.namespace(), CollectionId::namespace);
                finalCollectionFilter = finalCollectionFilter.or(signalDataCollectionPredicate);
            }
        }
        this.collectionFilter = finalCollectionFilter;

        // Define the field selector that provides the field filter to exclude or rename fields in a document ...
        fieldSelector = FieldSelector.builder()
                .excludeFields(config.getFieldExcludeList())
                .renameFields(config.getFieldRenames())
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

    private boolean isNotBuiltIn(CollectionId id) {
        return !config.getBuiltInDbNames().contains(id.dbName());
    }

    public FilterConfig getConfig() {
        return config;
    }

    public static class FilterConfig {

        private static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet("local", "admin", "config");

        private final String dbIncludeList;
        private final String dbExcludeList;
        private final String collectionIncludeList;
        private final String collectionExcludeList;
        private final String fieldRenames;
        private final String fieldExcludeList;
        private final String signalDataCollection;
        private final ChangeStreamPipeline userPipeline;

        public FilterConfig(Configuration config) {
            this.dbIncludeList = resolveString(config, MongoDbConnectorConfig.DATABASE_INCLUDE_LIST);
            this.dbExcludeList = resolveString(config, MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST);
            this.collectionIncludeList = resolveString(config, MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST);
            this.collectionExcludeList = resolveString(config, MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST);
            this.fieldRenames = resolveString(config, MongoDbConnectorConfig.FIELD_RENAMES);
            this.fieldExcludeList = resolveString(config, MongoDbConnectorConfig.FIELD_EXCLUDE_LIST);
            this.signalDataCollection = resolveString(config, MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION);
            this.userPipeline = resolveChangeStreamPipeline(config, MongoDbConnectorConfig.CURSOR_PIPELINE);
        }

        public String getDbIncludeList() {
            return dbIncludeList;
        }

        public String getDbExcludeList() {
            return dbExcludeList;
        }

        public String getCollectionIncludeList() {
            return collectionIncludeList;
        }

        public String getCollectionExcludeList() {
            return collectionExcludeList;
        }

        public String getFieldRenames() {
            return fieldRenames;
        }

        public String getFieldExcludeList() {
            return fieldExcludeList;
        }

        public String getSignalDataCollection() {
            return signalDataCollection;
        }

        public Set<String> getBuiltInDbNames() {
            return BUILT_IN_DB_NAMES;
        }

        public ChangeStreamPipeline getUserPipeline() {
            return userPipeline;
        }

        private static String resolveString(Configuration config, Field key) {
            return normalize(config.getString(key));
        }

        private static ChangeStreamPipeline resolveChangeStreamPipeline(Configuration config, Field field) {
            var text = config.getString(field);
            return new ChangeStreamPipeline(text);
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

    }

}
