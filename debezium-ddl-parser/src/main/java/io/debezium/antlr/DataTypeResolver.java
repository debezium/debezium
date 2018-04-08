/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.antlr;

import org.antlr.v4.runtime.ParserRuleContext;

import java.sql.Types;
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

    public Integer resolveDataType(ParserRuleContext dataTypeContext) {
        for (DataTypeEntry dataTypeEntry : contextDataTypesMap.get(dataTypeContext.getClass().getCanonicalName())) {
            if (dataTypeContext.getToken(dataTypeEntry.getDbmsDataTypeTokenIdentifier(), 0) != null) {
                return dataTypeEntry.getJdbcDataType();
            }
        }
        return Types.NULL;
    }

    public static class DataTypeEntry {
        private final int dbmsDataTypeTokenIdentifier;
        private final int jdbcDataType;

        public DataTypeEntry(int dbmsDataTypeTokenIdentifier, int jdbcDataType) {
            this.dbmsDataTypeTokenIdentifier = dbmsDataTypeTokenIdentifier;
            this.jdbcDataType = jdbcDataType;
        }

        public int getDbmsDataTypeTokenIdentifier() {
            return dbmsDataTypeTokenIdentifier;
        }

        public int getJdbcDataType() {
            return jdbcDataType;
        }
    }
}
