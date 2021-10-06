/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.annotation.Immutable;
import io.debezium.config.Configuration;
import io.debezium.function.Predicates;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnId;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Selectors;
import io.debezium.relational.Selectors.TableIdToStringMapper;
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
    public static Builder build() {
        return new Builder(null);
    }

    /**
     * Builds a new {@link ColumnMappers} instance based on the given configuration.
     */
    public static ColumnMappers create(RelationalDatabaseConnectorConfig connectorConfig) {
        final Builder builder = new Builder(connectorConfig.getTableIdMapper());
        final Configuration config = connectorConfig.getConfig();

        // Define the truncated, masked, and mapped columns ...
        config.forEachMatchingFieldNameWithInteger("column\\.truncate\\.to\\.(\\d+)\\.chars", builder::truncateStrings);
        config.forEachMatchingFieldNameWithInteger("column\\.mask\\.with\\.(\\d+)\\.chars", builder::maskStrings);
        config.forEachMatchingFieldName("column\\.propagate\\.source\\.type", builder::propagateSourceTypeToSchemaParameter);
        config.forEachMatchingFieldName("datatype\\.propagate\\.source\\.type", builder::propagateSourceTypeToSchemaParameterByDatatype);

        final Pattern hashAlgorithmAndSaltExtractPattern = Pattern.compile("((?<hashAlgorithm>[^.]+)\\.with\\.salt\\.(?<salt>.+))");
        config.forEachMatchingFieldNameWithString("column\\.mask\\.hash\\." + hashAlgorithmAndSaltExtractPattern.pattern(),
                (fullyQualifiedColumnNames, hashAlgorithmAndSalt) -> {
                    Matcher matcher = hashAlgorithmAndSaltExtractPattern.matcher(hashAlgorithmAndSalt);
                    if (matcher.matches()) {
                        builder.maskStringsByHashing(fullyQualifiedColumnNames, matcher.group("hashAlgorithm"), matcher.group("salt"));
                    }
                });

        config.forEachMatchingFieldNameWithString("column\\.mask\\.hash.v2.\\." + hashAlgorithmAndSaltExtractPattern.pattern(),
                (fullyQualifiedColumnNames, hashAlgorithmAndSalt) -> {
                    Matcher matcher = hashAlgorithmAndSaltExtractPattern.matcher(hashAlgorithmAndSalt);
                    if (matcher.matches()) {
                        builder.maskStringsByHashingV2(fullyQualifiedColumnNames, matcher.group("hashAlgorithm"), matcher.group("salt"));
                    }
                });

        return builder.build();
    }

    /**
     * A builder of {@link Selectors}.
     *
     * @author Randall Hauch
     */
    public static class Builder {

        private final List<MapperRule> rules = new ArrayList<>();
        private final TableIdToStringMapper tableIdMapper;

        public Builder(TableIdToStringMapper tableIdMapper) {
            this.tableIdMapper = tableIdMapper;
        }

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
            BiPredicate<TableId, Column> columnMatcher = Predicates.includes(fullyQualifiedColumnNames, (tableId, column) -> fullyQualifiedColumnName(tableId, column));
            rules.add(new MapperRule(columnMatcher, mapper));
            if (tableIdMapper != null) {
                columnMatcher = Predicates.includes(fullyQualifiedColumnNames, (tableId, column) -> mappedTableColumnName(tableId, column));
                rules.add(new MapperRule(columnMatcher, mapper));
            }
            return this;
        }

        public String fullyQualifiedColumnName(TableId tableId, Column column) {
            ColumnId id = new ColumnId(tableId, column.name());
            return id.toString();
        }

        public String mappedTableColumnName(TableId tableId, Column column) {
            ColumnId id = new ColumnId(mappedTableId(tableId), column.name());
            return id.toString();
        }

        public Builder mapByDatatype(String columnDatatypes, ColumnMapper mapper) {
            BiPredicate<TableId, Column> columnMatcher = Predicates.includes(columnDatatypes, (tableId, column) -> fullyQualifiedColumnDatatype(tableId, column));
            rules.add(new MapperRule(columnMatcher, mapper));
            if (tableIdMapper != null) {
                columnMatcher = Predicates.includes(columnDatatypes, (tableId, column) -> mappedTableColumnDatatype(tableId, column));
                rules.add(new MapperRule(columnMatcher, mapper));
            }
            return this;
        }

        public String fullyQualifiedColumnDatatype(TableId tableId, Column column) {
            return tableId.toString() + "." + column.typeName();
        }

        public String mappedTableColumnDatatype(TableId tableId, Column column) {
            return mappedTableId(tableId).toString() + "." + column.typeName();
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
            return map(fullyQualifiedColumnNames, mapperClass, null);
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

        public Builder maskStringsByHashing(String fullyQualifiedColumnNames, String hashAlgorithm, String salt) {
            return map(fullyQualifiedColumnNames, new MaskStrings(salt.getBytes(), hashAlgorithm, MaskStrings.HashingByteArrayStrategy.V1));
        }

        public Builder maskStringsByHashingV2(String fullyQualifiedColumnNames, String hashAlgorithm, String salt) {
            return map(fullyQualifiedColumnNames, new MaskStrings(salt.getBytes(), hashAlgorithm, MaskStrings.HashingByteArrayStrategy.V2));
        }

        public Builder propagateSourceTypeToSchemaParameter(String fullyQualifiedColumnNames, String value) {
            return map(value, new PropagateSourceTypeToSchemaParameter());
        }

        public Builder propagateSourceTypeToSchemaParameterByDatatype(String columnDatatypes, String value) {
            return mapByDatatype(value, new PropagateSourceTypeToSchemaParameter());
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
            return map(fullyQualifiedColumnNames, mapperClassName, null);
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
                }
                catch (ClassNotFoundException e) {
                    throw new ConnectException("Unable to find column mapper class " + mapperClassName + ": " + e.getMessage(), e);
                }
                catch (ClassCastException e) {
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

        private TableId mappedTableId(TableId tableId) {
            return new TableId(tableId.catalog(), tableId.schema(), tableId.table(), tableIdMapper);
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
        ColumnMapper mapper = mapperFor(tableId, column);
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
        Optional<MapperRule> matchingRule = rules.stream().filter(rule -> rule.matches(tableId, column)).findFirst();
        if (matchingRule.isPresent()) {
            return matchingRule.get().mapper;
        }
        return null;
    }

    @Immutable
    protected static final class MapperRule {
        protected final BiPredicate<TableId, Column> predicate;
        protected final ColumnMapper mapper;

        protected MapperRule(BiPredicate<TableId, Column> predicate, ColumnMapper mapper) {
            this.predicate = predicate;
            this.mapper = mapper;
        }

        protected boolean matches(TableId tableId, Column column) {
            return predicate.test(tableId, column);
        }
    }

    protected static ColumnMapper instantiateMapper(Class<ColumnMapper> clazz, Configuration config) {
        try {
            ColumnMapper mapper = clazz.getDeclaredConstructor().newInstance();
            if (config != null) {
                mapper.initialize(config);
            }
            return mapper;
        }
        catch (InstantiationException e) {
            throw new ConnectException("Unable to instantiate column mapper class " + clazz.getName() + ": " + e.getMessage(), e);
        }
        catch (IllegalAccessException e) {
            throw new ConnectException("Unable to access column mapper class " + clazz.getName() + ": " + e.getMessage(), e);
        }
        catch (Throwable e) {
            throw new ConnectException("Unable to initialize the column mapper class " + clazz.getName() + ": " + e.getMessage(), e);
        }
    }
}
