/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import io.debezium.relational.ddl.DataType;
import io.debezium.relational.ddl.DataTypeBuilder;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTypeResolver {

    private final Map<String, List<DataTypeEntry>> contextDataTypesMap = new HashMap<>();

    public void registerDataTypes(String contextClassCanonicalName, List<DataTypeEntry> dataTypeEntries) {
        contextDataTypesMap.put(contextClassCanonicalName, dataTypeEntries);
    }

    public void registerDataTypes(String contextClassCanonicalName, DataTypeEntry dataTypeEntry) {
        List<DataTypeEntry> dataTypeEntries = contextDataTypesMap.computeIfAbsent(contextClassCanonicalName, k -> new ArrayList<>());
        dataTypeEntries.add(dataTypeEntry);
    }

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
                    addOptionalSuffixToName(dataTypeContext, dataTypeEntry, dataTypeBuilder);
                    dataTypeBuilder.jdbcType(dataTypeEntry.getJdbcDataType());
                    dataTypeBuilder.length(dataTypeEntry.getDefaultLength());
                    dataTypeBuilder.scale(dataTypeEntry.getDefaultScale());

                    dataType = dataTypeBuilder.create();
                    selectedTypePriority = dataTypePriority;
                }
            }
        }
        if (dataType == null) {
            throw new ParsingException(null, "Unrecognized dataType for " + AntlrDdlParser.getText(dataTypeContext));
        }
        return dataType;
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

    public static class DataTypeEntry {

        /**
         * Token identifiers for DBMS data type
         */
        private final Integer[] dbmsDataTypeTokenIdentifiers;
        /**
         * Mapped JDBC data type
         */
        private final int jdbcDataType;
        private Integer[] suffixTokens = null;
        private int defaultLength = -1;
        private int defaultScale = -1;

        public DataTypeEntry(int jdbcDataType, Integer... dbmsDataTypeTokenIdentifiers) {
            this.dbmsDataTypeTokenIdentifiers = dbmsDataTypeTokenIdentifiers;
            this.jdbcDataType = jdbcDataType;
        }

        public Integer[] getDbmsDataTypeTokenIdentifiers() {
            return dbmsDataTypeTokenIdentifiers;
        }

        public int getJdbcDataType() {
            return jdbcDataType;
        }

        public Integer[] getSuffixTokens() {
            return suffixTokens;
        }

        public int getDefaultLength() {
            return defaultLength;
        }

        public int getDefaultScale() {
            return defaultScale;
        }

        public DataTypeEntry setSuffixTokens(Integer... suffixTokens) {
            this.suffixTokens = suffixTokens;
            return this;
        }

        public DataTypeEntry setDefualtLengthDimmension(int defaultLength) {
            this.defaultLength = defaultLength;
            return this;
        }

        public DataTypeEntry setDefualtLengthScaleDimmension(int defaultLength, int defaultScale) {
            this.defaultLength = defaultLength;
            this.defaultScale = defaultScale;
            return this;
        }

    }
}
