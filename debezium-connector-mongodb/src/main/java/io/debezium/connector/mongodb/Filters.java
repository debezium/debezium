/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mongodb.FieldSelector.FieldFilter;
import io.debezium.connector.mongodb.MongoDbConnectorConfig.FiltersMatchMode;
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
        this.databaseFilter = createDatabaseFilter();
        this.collectionFilter = createCollectionFilter();
        this.fieldSelector = createFieldSelector();
    }

    private Predicate<String> createDatabaseFilter() {
        var dbIncludeList = config.getDbIncludeList();
        var dbExcludeList = config.getDbExcludeList();

        return Optional.<Predicate<String>> empty()
                .or(() -> dbIncludeList.map(value -> includes(value, s -> s)))
                .or(() -> dbExcludeList.map(value -> excludes(value, s -> s)))
                .orElse((db) -> true);
    }

    private Predicate<CollectionId> createCollectionFilter() {
        var collectionIncludeList = config.getCollectionIncludeList();
        var collectionExcludeList = config.getCollectionExcludeList();

        final Predicate<CollectionId> collectionFilter = Optional.<Predicate<CollectionId>> empty()
                .or(() -> collectionIncludeList.map(list -> includes(list, CollectionId::namespace)))
                .or(() -> collectionExcludeList.map(list -> excludes(list, CollectionId::namespace)))
                .orElse((id) -> true)
                .and(this::isNotBuiltIn);

        // Create signal collection filter if specified and not included
        Optional<Predicate<CollectionId>> signalCollectionFilter = config.getSignalDataCollection()
                .map(CollectionId::parse)
                .filter(id -> !collectionFilter.test(id))
                .map(id -> Predicates.includes(id.namespace(), CollectionId::namespace));

        // Combine signal filter and collection filter
        return signalCollectionFilter.map(collectionFilter::or).orElse(collectionFilter);
    }

    private FieldSelector createFieldSelector() {
        // Define the field selector that provides the field filter to exclude or rename fields in a document ...
        return FieldSelector.builder()
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

    private <T> Predicate<T> includes(String literalsOrPatterns, Function<T, String> conversion) {
        if (config.isLiteralsMatchMode()) {
            return Predicates.includesLiterals(literalsOrPatterns, conversion, true);
        }
        return Predicates.includes(literalsOrPatterns, conversion);
    }

    private <T> Predicate<T> excludes(String literalsOrPatterns, Function<T, String> conversion) {
        if (config.isLiteralsMatchMode()) {
            return Predicates.excludesLiterals(literalsOrPatterns, conversion, true);
        }
        return Predicates.excludes(literalsOrPatterns, conversion);
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
        private final FiltersMatchMode filtersMatchMode;
        private final boolean literalMatchMode;

        public FilterConfig(Configuration config) {
            var connectorConfig = new MongoDbConnectorConfig(config);
            this.dbIncludeList = resolveString(config, MongoDbConnectorConfig.DATABASE_INCLUDE_LIST);
            this.dbExcludeList = resolveString(config, MongoDbConnectorConfig.DATABASE_EXCLUDE_LIST);
            this.collectionIncludeList = resolveString(config, MongoDbConnectorConfig.COLLECTION_INCLUDE_LIST);
            this.collectionExcludeList = resolveString(config, MongoDbConnectorConfig.COLLECTION_EXCLUDE_LIST);
            this.fieldRenames = resolveString(config, MongoDbConnectorConfig.FIELD_RENAMES);
            this.fieldExcludeList = resolveString(config, MongoDbConnectorConfig.FIELD_EXCLUDE_LIST);
            this.signalDataCollection = resolveString(config, MongoDbConnectorConfig.SIGNAL_DATA_COLLECTION);
            this.userPipeline = resolveChangeStreamPipeline(config, MongoDbConnectorConfig.CURSOR_PIPELINE);
            this.filtersMatchMode = connectorConfig.getFiltersMatchMode();
            this.literalMatchMode = FiltersMatchMode.LITERAL.equals(filtersMatchMode);
        }

        public Optional<String> getDbIncludeList() {
            return Optional.ofNullable(dbIncludeList);
        }

        public Optional<String> getDbExcludeList() {
            return Optional.ofNullable(dbExcludeList);
        }

        public Optional<String> getCollectionIncludeList() {
            return Optional.ofNullable(collectionIncludeList);
        }

        public Optional<String> getCollectionExcludeList() {
            return Optional.ofNullable(collectionExcludeList);
        }

        public String getFieldRenames() {
            return fieldRenames;
        }

        public String getFieldExcludeList() {
            return fieldExcludeList;
        }

        public Optional<String> getSignalDataCollection() {
            return Optional.ofNullable(signalDataCollection);
        }

        public Set<String> getBuiltInDbNames() {
            return BUILT_IN_DB_NAMES;
        }

        public ChangeStreamPipeline getUserPipeline() {
            return userPipeline;
        }

        public FiltersMatchMode getFiltersMatchMode() {
            return filtersMatchMode;
        }

        public boolean isLiteralsMatchMode() {
            return literalMatchMode;
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
