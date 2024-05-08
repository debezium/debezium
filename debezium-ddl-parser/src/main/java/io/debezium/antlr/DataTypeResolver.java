/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.ddl.DataType;
import io.debezium.relational.ddl.DataTypeBuilder;
import io.debezium.text.ParsingException;

/**
 * A resolver for DBMS data types.
 *
 * Its main purpose is to match corresponding JDBC data type, resolve a name of parsed data type,
 * and optionally predefine default values for length and scale for DBMS data type.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
@ThreadSafe
@Immutable
public class DataTypeResolver {

    private final Map<String, List<DataTypeEntry>> contextDataTypesMap;

    private DataTypeResolver(Map<String, List<DataTypeEntry>> contextDataTypesMap) {
        this.contextDataTypesMap = Collections.unmodifiableMap(contextDataTypesMap);
    }

    /**
     * Resolves a data type from given parsed context.
     *
     * @param dataTypeContext parse context; may not e null
     * @return instance of {@link DataType}, which will holds matched JDBC type, name and default values for length and scale.
     */
    public DataType resolveDataType(ParserRuleContext dataTypeContext) {
        DataType dataType = null;
        // use priority according to number of matched tokens
        int selectedTypePriority = -1;

        for (DataTypeEntry dataTypeEntry : contextDataTypesMap.get(dataTypeContext.getClass().getCanonicalName())) {
            int dataTypePriority = dataTypeEntry.getDbmsDataTypeTokenIdentifiers().length;
            if (dataTypePriority > selectedTypePriority) {
                DataTypeBuilder dataTypeBuilder = new DataTypeBuilder();
                boolean correctDataType = true;
                for (Integer mainTokenIdentifier : dataTypeEntry.getDbmsDataTypeTokenIdentifiers()) {
                    TerminalNode token = dataTypeContext.getToken(mainTokenIdentifier, 0);
                    if (correctDataType) {
                        if (token == null) {
                            correctDataType = false;
                        }
                        else {
                            dataTypeBuilder.addToName(token.getText());
                        }
                    }
                }
                if (correctDataType) {
                    dataType = buildDataType(dataTypeContext, dataTypeEntry, dataTypeBuilder);
                    selectedTypePriority = dataTypePriority;
                }
            }
        }
        if (dataType == null) {
            throw new ParsingException(null, "Unrecognized dataType for " + AntlrDdlParser.getText(dataTypeContext));
        }
        return dataType;
    }

    private DataType buildDataType(ParserRuleContext dataTypeContext, DataTypeEntry dataTypeEntry, DataTypeBuilder dataTypeBuilder) {
        addOptionalSuffixToName(dataTypeContext, dataTypeEntry, dataTypeBuilder);
        dataTypeBuilder.jdbcType(dataTypeEntry.getJdbcDataType());
        dataTypeBuilder.length(dataTypeEntry.getDefaultLength());
        dataTypeBuilder.scale(dataTypeEntry.getDefaultScale());

        return dataTypeBuilder.create();
    }

    private void addOptionalSuffixToName(ParserRuleContext dataTypeContext, DataTypeEntry dataTypeEntry, DataTypeBuilder dataTypeBuilder) {
        if (dataTypeEntry.getSuffixTokens() != null) {
            for (Integer suffixTokenIdentifier : dataTypeEntry.getSuffixTokens()) {
                if (dataTypeContext.getToken(suffixTokenIdentifier, 0) != null) {
                    dataTypeBuilder.addToName(dataTypeContext.getToken(suffixTokenIdentifier, 0).getText());
                }
            }
        }
    }

    public static class Builder {

        private final Map<String, List<DataTypeEntry>> contextDataTypesMap = new HashMap<>();

        /**
         * Registers a data type entries, which will be used for resolving phase.
         *
         * @param contextClassCanonicalName canonical name of context instance, in which the data type entry will appear; may not be null
         * @param dataTypeEntries list of {@link DataTypeEntry} definitions; may not be null
         */
        public void registerDataTypes(String contextClassCanonicalName, List<DataTypeEntry> dataTypeEntries) {
            contextDataTypesMap.put(contextClassCanonicalName, Collections.unmodifiableList(dataTypeEntries));
        }

        public DataTypeResolver build() {
            return new DataTypeResolver(contextDataTypesMap);
        }
    }

    /**
     * DTO class for definition of data type.
     */
    public static class DataTypeEntry {

        /**
         * The corresponding JDBC data type
         */
        private final int jdbcDataType;
        /**
         * Token identifiers for DBMS data type
         */
        private final Integer[] dbmsDataTypeTokenIdentifiers;
        /**
         * Token identifiers for optional suffix tokens for DBMS data type.
         */
        private Integer[] suffixTokens = null;
        private int defaultLength = Column.UNSET_INT_VALUE;
        private int defaultScale = Column.UNSET_INT_VALUE;

        public DataTypeEntry(int jdbcDataType, Integer... dbmsDataTypeTokenIdentifiers) {
            this.dbmsDataTypeTokenIdentifiers = dbmsDataTypeTokenIdentifiers;
            this.jdbcDataType = jdbcDataType;
        }

        Integer[] getDbmsDataTypeTokenIdentifiers() {
            return dbmsDataTypeTokenIdentifiers;
        }

        int getJdbcDataType() {
            return jdbcDataType;
        }

        Integer[] getSuffixTokens() {
            return suffixTokens;
        }

        int getDefaultLength() {
            return defaultLength;
        }

        int getDefaultScale() {
            return defaultScale;
        }

        /**
         * Sets an optional suffix tokens that may appear in DBMS data type definition.
         *
         * @param suffixTokens optional suffix tokens.
         * @return instance of this class, so the calls may be chained.
         */
        public DataTypeEntry setSuffixTokens(Integer... suffixTokens) {
            this.suffixTokens = suffixTokens;
            return this;
        }

        /**
         * Set a default length for data type.
         *
         * @param defaultLength default length for data type.
         * @return instance of this class, so the calls may be chained.
         */
        public DataTypeEntry setDefaultLengthDimension(int defaultLength) {
            this.defaultLength = defaultLength;
            return this;
        }

        /**
         * Set a default length and scale for data type.
         *
         * @param defaultLength default length for data type.
         * @param defaultScale default scale for data type.
         * @return instance of this class, so the calls may be chained.
         */
        public DataTypeEntry setDefaultLengthScaleDimension(int defaultLength, int defaultScale) {
            this.defaultLength = defaultLength;
            this.defaultScale = defaultScale;
            return this;
        }

        @Override
        public String toString() {
            return "DataTypeEntry [jdbcDataType=" + jdbcDataType + ", dbmsDataTypeTokenIdentifiers="
                    + Arrays.toString(dbmsDataTypeTokenIdentifiers) + ", suffixTokens=" + Arrays.toString(suffixTokens)
                    + ", defaultLength=" + defaultLength + ", defaultScale=" + defaultScale + "]";
        }
    }
}
