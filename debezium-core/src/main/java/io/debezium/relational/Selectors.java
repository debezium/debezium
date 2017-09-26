/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.annotation.Immutable;
import io.debezium.function.Predicates;

/**
 * Define predicates determines whether tables or columns should be used. The predicates use rules to determine which tables
 * and columns are included or excluded.
 * <p>
 * Because tables can be included and excluded based upon their fully-qualified names and based upon the database names, this
 * class defines a {@link #tableSelector() builder} to collect the various regular expression patterns the predicate(s) will use
 * to determine which columns and tables are included. The builder is then used to
 * {@link TableSelectionPredicateBuilder#build() build} the immutable table selection predicate.
 * <p>
 * By default all columns in included tables will be selected, except when they are specifically excluded using regular
 * expressions that match the columns' fully-qualified names. Therefore, the predicate is constructed using a simple
 * {@link #excludeColumns(String) static method}.
 * 
 * @author Randall Hauch
 */
@Immutable
public class Selectors {
    
    /**
     * Obtain a new {@link TableSelectionPredicateBuilder builder} for a table selection predicate.
     * 
     * @return the builder; never null
     */
    public static DatabaseSelectionPredicateBuilder databaseSelector() {
        return new DatabaseSelectionPredicateBuilder();
    }
    
    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
    
    /**
     * A builder of a database predicate.
     */
    public static class DatabaseSelectionPredicateBuilder {
        private Predicate<String> dbInclusions;
        private Predicate<String> dbExclusions;

        /**
         * Specify the names of the databases that should be included. This method will override previously included and
         * excluded databases.
         * 
         * @param databaseNames the comma-separated list of database names to include; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public DatabaseSelectionPredicateBuilder includeDatabases(String databaseNames) {
            if (databaseNames == null || databaseNames.trim().isEmpty()) {
                dbInclusions = null;
            } else {
                dbInclusions = Predicates.includes(databaseNames);
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be included. This method will override previously included and
         * excluded databases.
         *
         * @param databaseNames A Collection of database names to include; may be null or empty.
         * @return this builder so that methods can be chained together; never null
         */
        public DatabaseSelectionPredicateBuilder includeDatabases(Collection<String> databaseNames) {
            if (databaseNames == null || databaseNames.isEmpty()) {
                dbInclusions = null;
            } else {
                includeDatabases(String.join(",", databaseNames));
            }
            return this;
        }

        /**
         * Add a single database that should be included.
         * @param databaseName the database name to be included. Cannot be null. This method will override database
         *                     exclusion.
         * @return this builder so that methods can be chained together; never null
         */
        public DatabaseSelectionPredicateBuilder includeSingleDatabase(String databaseName) {
            Predicate<String> includeDatabase = Predicates.includes(databaseName);
            if (dbInclusions != null) {
                dbInclusions = dbInclusions.or(includeDatabase);
            } else {
                dbInclusions = includeDatabase;
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be excluded. This method will override previously {@link
         * #excludeDatabases(String) excluded} databases, although {@link #includeDatabases(String) including databases} overrides
         * exclusions.
         * 
         * @param databaseNames the comma-separated list of database names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public DatabaseSelectionPredicateBuilder excludeDatabases(String databaseNames) {
            if (databaseNames == null || databaseNames.trim().isEmpty()) {
                dbExclusions = null;
            } else {
                dbExclusions = Predicates.excludes(databaseNames);
            }
            return this;
        }

        public DatabaseSelectionPredicateBuilder excludeDatabases(Collection<String> databaseNames) {
            if (databaseNames == null || databaseNames.isEmpty()) {
                dbInclusions = null;
            } else {
                excludeDatabases(String.join(",", databaseNames));
            }
            return this;
        }

        /**
         * Add a single database that should be excluded. An include will override an exclusion.
         * @param databaseName the database name to be excluded.  Cannot be null.
         * @return
         */
        public DatabaseSelectionPredicateBuilder excludeSingleDatabase(String databaseName) {
            Predicate<String> excludeDatabase = Predicates.includes(databaseName);
            if (dbExclusions != null){
                dbExclusions = dbExclusions.and(excludeDatabase); // TODO see {@link #includeDatabase(String)}
            } else {
                dbExclusions = excludeDatabase;
            }
            return this;
        }

        /**
         * Build the {@link Predicate} that determines whether a database identified by its name is to be included.
         * 
         * @return the table selection predicate; never null
         * @see #includeDatabases(String)
         * @see #excludeDatabases(String)
         */
        public Predicate<String> build() {
            Predicate<String> dbFilter = dbInclusions != null ? dbInclusions : dbExclusions;
            return dbFilter != null ? dbFilter : (id) -> true;
        }
    }

    /**
     * Obtain a new {@link TableSelectionPredicateBuilder builder} for a table selection predicate.
     * 
     * @return the builder; never null
     */
    public static TableSelectionPredicateBuilder tableSelector() {
        return new TableSelectionPredicateBuilder();
    }

    /**
     * A builder of a table predicate.
     */
    public static class TableSelectionPredicateBuilder {
        private Predicate<String> dbInclusions;
        private Predicate<String> dbExclusions;
        private Predicate<String> schemaInclusions;
        private Predicate<String> schemaExclusions;
        private Predicate<TableId> tableInclusions;
        private Predicate<TableId> tableExclusions;

        /**
         * Specify the names of the databases that should be included. This method will override previously included and
         * excluded databases.
         * 
         * @param databaseNames the comma-separated list of database names to include; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeDatabases(String databaseNames) {
            if (isEmpty(databaseNames)) {
                dbInclusions = null;
            } else {
                dbInclusions = Predicates.includes(databaseNames);
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be included. This method will override previously included and
         * excluded databases.
         *
         * @param databaseNames the collection of database names to include; may be null or empty
         * @return
         */
        public TableSelectionPredicateBuilder includeDatabases(Collection<String> databaseNames) {
            if (databaseNames == null || databaseNames.isEmpty()){
                dbInclusions = null;
            } else {
                includeDatabases(String.join(",", databaseNames));
            }
            return this;
        }

        /**
         * Add a single database that should be included.
         * @param databaseName the database name to be included. Cannot be null. This method will override database
         *                     exclusion.
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeSingleDatabase(String databaseName) {
            Predicate<String> includeDatabase = Predicates.includes(databaseName);
            if (dbInclusions != null) {
                dbInclusions = dbInclusions.or(includeDatabase);
            } else {
                dbInclusions = includeDatabase;
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be excluded. This method will override previously
         * excluded databases, although including databases overrides exclusions.
         * 
         * @param databaseNames the comma-separated list of database names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeDatabases(String databaseNames) {
            if (isEmpty(databaseNames)) {
                dbExclusions = null;
            } else {
                dbExclusions = Predicates.excludes(databaseNames);
            }
            return this;
        }

        /**
         * Specify the names of the databases that should be excluded. This method will override previously
         * excluded databases, although including databases overrides exclusions.
         *
         * @param databaseNames the comma-separated list of database names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeDatabases(Collection<String> databaseNames) {
            if (databaseNames == null || databaseNames.isEmpty()) {
                dbExclusions = null;
            } else {
                excludeDatabases(String.join(",", databaseNames));
            }
            return this;
        }

        /**
         * Add a single database that should be excluded. Including databases overrrides exclusions.
         * @param databaseName the database name to be excluded. Cannot be null.
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeSingleDatabase(String databaseName) {
            Predicate<String> includeDatabase = Predicates.includes(databaseName);
            if (dbInclusions != null) {
                dbInclusions = dbInclusions.or(includeDatabase);
            } else {
                dbInclusions = includeDatabase;
            }
            return this;
        }
    
        /**
         * Specify the names of the schemas that should be included. This method will override previously included and
         * {@link #excludeSchemas(String) excluded} schemas.
         *
         * @param schemaNames the comma-separated list of schema names to include; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeSchemas(String schemaNames) {
            if (isEmpty(schemaNames)) {
                schemaInclusions = null;
            } else {
                schemaInclusions = Predicates.includes(schemaNames);
            }
            return this;
        }
    
        /**
         * Specify the names of the schemas that should be excluded. This method will override previously {@link
         * #excludeSchemas(String) excluded} schemas, although {@link #includeSchemas(String)} including schemas} overrides
         * exclusions.
         *
         * @param schemaNames the comma-separated list of schema names to exclude; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeSchemas(String schemaNames) {
            if (isEmpty(schemaNames)) {
                schemaExclusions = null;
            } else {
                schemaExclusions = Predicates.excludes(schemaNames);
            }
            return this;
        }

        /**
         * Specify the names of the tables that should be included. This method will override previously included and
         * excluded table names.
         * <p>
         * Note that any specified tables that are in an excluded database will not be included.
         * 
         * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to include; may be null or
         *            empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder includeTables(String fullyQualifiedTableNames) {
            if (isEmpty(fullyQualifiedTableNames)) {
                tableInclusions = null;
            } else {
                tableInclusions = Predicates.includes(fullyQualifiedTableNames, TableId::toString);
            }
            return this;
        }

        /**
         * Specify the names of the tables that should be included. This method will override previously included and
         * excluded table names.
         * <p>
         * Note that any specified tables that are in an excluded database will not be included.
         *
         * @param tables the list of {@link TableId}s to include; may be null or empty.
         * @return this builder so that methods may be chained together; never null
         */
        public TableSelectionPredicateBuilder includeTables(Collection<TableId> tables) {
            if (tables == null || tables.isEmpty()) {
                tableInclusions = null;
            } else {
                Collection<String> tableStrings = tables.stream().map(TableId::toString).collect(Collectors.toCollection(ArrayList::new));
                includeTables(String.join(",", tableStrings));
            }
            return this;
        }

        /**
         * Specify a single table that should be included. This method will override exclusion.
         * <p>
         * Note that if this table is in an excluded database will not be included.
         *
         * @param table the {@link TableId} to include; may not be null.
         * @return this builder so that methods may be chained together; never null.
         */
        public TableSelectionPredicateBuilder includeSingleTable(TableId table) {
            Predicate<TableId> includeTable = Predicates.includes(table.toString(), TableId::toString);
            if (tableInclusions != null) {
                tableInclusions = tableInclusions.or(includeTable);
            } else {
                tableInclusions = includeTable;
            }
            return this;
        }

        /**
         * Specify the names of the tables that should be excluded. This method will override previously
         * excluded tables, although including tables overrides exclusions.
         * <p>
         * Note that any specified tables that are in an excluded database will not be included.
         * 
         * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to exclude; may be null or
         *            empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeTables(String fullyQualifiedTableNames) {
            if (isEmpty(fullyQualifiedTableNames)) {
                tableExclusions = null;
            } else {
                tableExclusions = Predicates.excludes(fullyQualifiedTableNames, TableId::toString);
            }
            return this;
        }

        /**
         * Specify the tables that should be excluded. This method will override previously excluded tables,
         * although including tables overrides exclusions.
         * <p>
         * Note that any specified tables that are in an excluded database will not be included.
         *
         * @param tables the list of tables to excluded; may be null or empty
         * @return this builder so that methods can be chained together; never null
         */
        public TableSelectionPredicateBuilder excludeTables(Collection<TableId> tables) {
            if (tables == null || tables.isEmpty()){
                tableExclusions = null;
            } else {
                Collection<String> tableStrings = tables.stream().map(TableId::toString).collect(Collectors.toCollection(ArrayList::new));
                excludeTables(String.join(",", tableStrings));
            }
            return this;
        }

        /**
         * Specify a single table that should be excluded. Including tables overrides exclusions.
         * <p>
         * Note that any tables that are in any excluded database will not be included.
         *
         * @param table the table to exclude; may not be null
         * @return this builder so that methods can be chained together; never null.
         */
        public TableSelectionPredicateBuilder excludeSingleTable(TableId table) {
            Predicate<TableId> includeTable = Predicates.excludes(table.toString(), TableId::toString);
            if (tableExclusions != null) {
                tableExclusions = tableExclusions.and(includeTable);
            } else {
                tableExclusions = includeTable;
            }
            return this;
        }

        /**
         * Build the {@link Predicate} that determines whether a table identified by a given {@link TableId} is to be included.
         * 
         * @return the table selection predicate; never null
         * @see #includeDatabases(String)
         * @see #excludeDatabases(String)
         * @see #includeTables(String)
         * @see #excludeTables(String) 
         * @see #includeSchemas(String) 
         * @see #excludeSchemas(String) 
         */
        public Predicate<TableId> build() {
            Predicate<TableId> tableFilter = tableInclusions != null ? tableInclusions : tableExclusions;
            Predicate<String> dbFilter = dbInclusions != null ? dbInclusions : dbExclusions;
            Predicate<String> schemaFilter = schemaInclusions != null ? schemaInclusions : schemaExclusions;
            
            if (dbFilter != null) {
                return buildStartingFromDbFilter(dbFilter, schemaFilter, tableFilter);
            }

            if (schemaFilter != null) {
                return buildStartingFromSchemaFilter(schemaFilter, tableFilter);
            }

            if (tableFilter != null) {
                return tableFilter;
            }
            
            return (id) -> true;
        }
    
        private Predicate<TableId> buildStartingFromSchemaFilter(Predicate<String> schemaFilter, Predicate<TableId> tableFilter) {
            assert schemaFilter != null;
            if (tableFilter != null) {
                return (id) -> schemaFilter.test(id.schema()) && tableFilter.test(id);
            } else {
                return (id) -> schemaFilter.test(id.schema());
            }
        }
    
        private Predicate<TableId> buildStartingFromDbFilter(Predicate<String> dbFilter, Predicate<String> schemaFilter,
                                                             Predicate<TableId> tableFilter) {
            assert dbFilter != null;
            
            if (schemaFilter != null) {
                if (tableFilter != null) {
                    return (id) -> dbFilter.test(id.catalog()) && schemaFilter.test(id.schema()) && tableFilter.test(id);
                } else {
                    return (id) -> schemaFilter.test(id.schema());                        
                }
            } else if (tableFilter != null) {
                return (id) -> dbFilter.test(id.catalog()) && tableFilter.test(id);
            } else {
                return (id) -> dbFilter.test(id.catalog());    
            }
        }
    }

    /**
     * Build the {@link Predicate} that determines whether a column identified by a given {@link ColumnId} is to be included,
     * using the given comma-separated regular expression patterns defining which columns (if any) should be <i>excluded</i>.
     * <p>
     * Note that this predicate is completely independent of the table selection predicate, so it is expected that this predicate
     * be used only <i>after</i> the table selection predicate determined the table containing the column(s) is to be used.
     * 
     * @param fullyQualifiedTableNames the comma-separated list of fully-qualified table names to exclude; may be null or
     *            empty
     * @return this builder so that methods can be chained together; never null
     */
    public static Predicate<ColumnId> excludeColumns(String fullyQualifiedTableNames) {
        return Predicates.excludes(fullyQualifiedTableNames, ColumnId::toString);
    }
}
