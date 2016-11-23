/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.function.Predicates;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnId;
import io.debezium.relational.Selectors;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Strings;

/**
 * A set of {@link ColumnMapper} objects for columns.
 * 
 * @author Randall Hauch
 */
@Immutable
public class ColumnMappers {

    /**
     * Obtain a new {@link Builder builder} for a table selection predicate.
     * 
     * @return the builder; never null
     */
    public static Builder create() {
        return new Builder();
    }

    /**
     * A builder of {@link Selectors}.
     * 
     * @author Randall Hauch
     */
    public static class Builder {

        private final List<MapperRule> rules = new ArrayList<>();

        /**
         * Set a mapping function for the columns with fully-qualified names that match the given comma-separated list of regular
         * expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param mapper the column mapping function that will be used to map actual values into values used in the output record;
         *            null if an existing mapping function should be removed
         * @return this object so that methods can be chained together; never null
         */
        public Builder map(String fullyQualifiedColumnNames, ColumnMapper mapper) {
            Predicate<ColumnId> columnMatcher = Predicates.includes(fullyQualifiedColumnNames, ColumnId::toString);
            rules.add(new MapperRule(columnMatcher, mapper));
            return this;
        }

        /**
         * Set a mapping function for the columns with fully-qualified names that match the given comma-separated list of regular
         * expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param mapperClass the Java class that implements {@code BiFunction<Column, Object, Object>} and that
         *            will be used to map actual values into values used in the output record; may not be null
         * @return this object so that methods can be chained together; never null
         */
        public Builder map(String fullyQualifiedColumnNames, Class<ColumnMapper> mapperClass) {
            return map(fullyQualifiedColumnNames,mapperClass,null);
        }

        /**
         * Set a mapping function for the columns with fully-qualified names that match the given comma-separated list of regular
         * expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param mapperClass the Java class that implements {@code BiFunction<Column, Object, Object>} and that
         *            will be used to map actual values into values used in the output record; may not be null
         * @param config the configuration to pass to the {@link ColumnMapper} instance; may be null
         * @return this object so that methods can be chained together; never null
         */
        public Builder map(String fullyQualifiedColumnNames, Class<ColumnMapper> mapperClass, Configuration config) {
            return map(fullyQualifiedColumnNames, instantiateMapper(mapperClass, config));
        }

        /**
         * Truncate to a maximum length the string values for each of the columns with the fully-qualified names.
         * Only columns {@link String} values can be truncated.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param maxLength the maximum number of characters to appear in the value
         * @return this object so that methods can be chained together; never null
         */
        public Builder truncateStrings(String fullyQualifiedColumnNames, int maxLength) {
            return map(fullyQualifiedColumnNames, new TruncateStrings(maxLength));
        }

        /**
         * Use a string of the specified number of '*' characters to mask the string values for each of the columns with
         * fully-qualified names that match the given comma-separated list of regular expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param numberOfChars the number of mask characters to be used in the mask value
         * @return this object so that methods can be chained together; never null
         */
        public Builder maskStrings(String fullyQualifiedColumnNames, int numberOfChars) {
            return maskStrings(fullyQualifiedColumnNames, numberOfChars, '*');
        }

        /**
         * Use a string of the specified number of characters to mask the string values for each of the columns with
         * fully-qualified names that match the given comma-separated list of regular expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param numberOfChars the number of mask characters to be used in the mask value
         * @param maskChar the character to be used; may not be null
         * @return this object so that methods can be chained together; never null
         */
        public Builder maskStrings(String fullyQualifiedColumnNames, int numberOfChars, char maskChar) {
            return maskStrings(fullyQualifiedColumnNames, Strings.createString(maskChar, numberOfChars));
        }

        /**
         * Use the specified string to mask the string values for each of the columns with
         * fully-qualified names that match the given comma-separated list of regular expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param maskValue the value to be used in place of the actual value; may not be null
         * @return this object so that methods can be chained together; never null
         */
        public Builder maskStrings(String fullyQualifiedColumnNames, String maskValue) {
            return map(fullyQualifiedColumnNames, new MaskStrings(maskValue));
        }

        /**
         * Set a mapping function for the columns with fully-qualified names that match the given comma-separated list of regular
         * expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param mapperClassName the name of the Java class that implements {@code BiFunction<Column, Object, Object>} and that
         *            will be used to map actual values into values used in the output record; null if
         *            an existing mapping function should be removed
         * @return this object so that methods can be chained together; never null
         */
        public Builder map(String fullyQualifiedColumnNames, String mapperClassName) {
            return map(fullyQualifiedColumnNames,mapperClassName,null);
        }

        /**
         * Set a mapping function for the columns with fully-qualified names that match the given comma-separated list of regular
         * expression patterns.
         * 
         * @param fullyQualifiedColumnNames the comma-separated list of fully-qualified column names; may not be null
         * @param mapperClassName the name of the Java class that implements {@code BiFunction<Column, Object, Object>} and that
         *            will be used to map actual values into values used in the output record; null if
         *            an existing mapping function should be removed
         * @param config the configuration to pass to the {@link ColumnMapper} instance; may be null
         * @return this object so that methods can be chained together; never null
         */
        @SuppressWarnings("unchecked")
        public Builder map(String fullyQualifiedColumnNames, String mapperClassName, Configuration config) {
            Class<ColumnMapper> mapperClass = null;
            if (mapperClassName != null) {
                try {
                    mapperClass = (Class<ColumnMapper>) getClass().getClassLoader().loadClass(mapperClassName);
                } catch (ClassNotFoundException e) {
                    throw new ConnectException("Unable to find column mapper class " + mapperClassName + ": " + e.getMessage(), e);
                } catch (ClassCastException e) {
                    throw new ConnectException(
                            "Column mapper class must implement " + ColumnMapper.class + " but does not: " + e.getMessage(),
                            e);
                }
            }
            return map(fullyQualifiedColumnNames, mapperClass, config);
        }

        /**
         * Build the {@link Predicate} that determines whether a table identified by a given {@link TableId} is to be included.
         * 
         * @return the table selection predicate; never null
         */
        public ColumnMappers build() {
            return new ColumnMappers(rules);
        }
    }

    private final List<MapperRule> rules;

    private ColumnMappers(List<MapperRule> rules) {
        assert rules != null;
        this.rules = new ArrayList<>(rules);
    }

    /**
     * Get the value mapping function for the given column.
     * 
     * @param table the table to which the column belongs; may not be null
     * @param column the column; may not be null
     * @return the mapping function, or null if there is no mapping function
     */
    public ValueConverter mappingConverterFor(Table table, Column column) {
        return mappingConverterFor(table.id(), column);
    }

    /**
     * Get the value mapping function for the given column.
     * 
     * @param tableId the identifier of the table to which the column belongs; may not be null
     * @param column the column; may not be null
     * @return the mapping function, or null if there is no mapping function
     */
    public ValueConverter mappingConverterFor(TableId tableId, Column column) {
        ColumnMapper mapper = mapperFor(tableId,column);
        return mapper != null ? mapper.create(column) : null;
    }

    /**
     * Get the value mapping function for the given column.
     * 
     * @param tableId the identifier of the table to which the column belongs; may not be null
     * @param column the column; may not be null
     * @return the mapping function, or null if there is no mapping function
     */
    public ColumnMapper mapperFor(TableId tableId, Column column) {
        ColumnId id = new ColumnId(tableId, column.name());
        Optional<MapperRule> matchingRule = rules.stream().filter(rule -> rule.matches(id)).findFirst();
        if (matchingRule.isPresent()) {
            return matchingRule.get().mapper;
        }
        return null;
    }

    @Immutable
    protected static final class MapperRule {
        protected final Predicate<ColumnId> predicate;
        protected final ColumnMapper mapper;

        protected MapperRule(Predicate<ColumnId> predicate, ColumnMapper mapper) {
            this.predicate = predicate;
            this.mapper = mapper;
        }

        protected boolean matches(ColumnId id) {
            return predicate.test(id);
        }
    }

    protected static ColumnMapper instantiateMapper(Class<ColumnMapper> clazz, Configuration config) {
        try {
             ColumnMapper mapper = clazz.newInstance();
             if ( config != null ) {
                 mapper.initialize(config);
             }
             return mapper;
        } catch (InstantiationException e) {
            throw new ConnectException("Unable to instantiate column mapper class " + clazz.getName() + ": " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new ConnectException("Unable to access column mapper class " + clazz.getName() + ": " + e.getMessage(), e);
        } catch (Throwable e) {
            throw new ConnectException("Unable to initialize the column mapper class " + clazz.getName() + ": " + e.getMessage(), e);
        }
    }
}
