/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.util;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.TransactionalBuffer;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValueImpl;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntry;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerDmlEntryImpl;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;

/**
 * @author csz
 * @version 1.0
 * {@code @date} 2022/7/13 17:49
 **/
public class DmlEventJsonUtil {

    public static String serialization(TransactionalBuffer.DmlEvent dmlEvent) {
        final JSONObject dmlEventObject = new JSONObject();
        dmlEventObject.put("operation", dmlEvent.getOperation());
        dmlEventObject.put("entry", serializationAsEntry(dmlEvent.getEntry()));
        dmlEventObject.put("scn", serializationAsScn(dmlEvent.getScn()));
        dmlEventObject.put("tableId", serializationAsTableId(dmlEvent.getTableId()));
        dmlEventObject.put("rowId", dmlEvent.getRowId());
        return dmlEventObject.toJSONString();
    }

    public static TransactionalBuffer.DmlEvent deSerialization(String jsonString) {
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }
        final JSONObject dmlEventObject = JSON.parseObject(jsonString);
        final int operation = dmlEventObject.getIntValue("operation");
        final LogMinerDmlEntry entry = deSerializationAsEntry(dmlEventObject.getJSONObject("entry"));
        final Scn scn = deSerializationAsScn(dmlEventObject.getJSONObject("scn"));
        final TableId tableId = deSerializationAsTableId(dmlEventObject.getJSONObject("tableId"));
        final String rowId = dmlEventObject.getString("rowId");
        return new TransactionalBuffer.DmlEvent(operation, entry, scn, tableId, rowId);
    }

    private static TableId deSerializationAsTableId(JSONObject tableIdJsonObject) {
        if (tableIdJsonObject == null) {
            return null;
        }
        final TableId tableId = new TableId(tableIdJsonObject.getString("catalogName"), tableIdJsonObject.getString("schemaName"),
                tableIdJsonObject.getString("tableName"));
        tableId.setId(tableIdJsonObject.getString("id"));
        return tableId;
    }

    private static LogMinerDmlEntry deSerializationAsEntry(JSONObject entryJsonObject) {
        if (entryJsonObject == null) {
            return null;
        }
        final LogMinerDmlEntry logMinerDmlEntry = new LogMinerDmlEntryImpl(deSerializationAsEntryAsCommandType(entryJsonObject.getString("commandType")),
                deSerializationAsEntryAsColumnValueList(entryJsonObject.getJSONArray("newLmColumnValues")),
                deSerializationAsEntryAsColumnValueList(entryJsonObject.getJSONArray("oldLmColumnValues")));
        logMinerDmlEntry.setObjectOwner(entryJsonObject.getString("objectOwner"));
        logMinerDmlEntry.setObjectName(entryJsonObject.getString("objectName"));
        final Long sourceTime = entryJsonObject.getLong("sourceTime");
        logMinerDmlEntry.setSourceTime(sourceTime == null ? null : new Timestamp(sourceTime));
        logMinerDmlEntry.setTransactionId(entryJsonObject.getString("transactionId"));
        logMinerDmlEntry.setScn(deSerializationAsScn(entryJsonObject.getJSONObject("scn")));
        logMinerDmlEntry.setRowId(entryJsonObject.getString("rowId"));
        return logMinerDmlEntry;
    }

    private static List<LogMinerColumnValue> deSerializationAsEntryAsColumnValueList(JSONArray columnValueJsonArray) {
        if (columnValueJsonArray == null) {
            return null;
        }
        final List<LogMinerColumnValue> result = new ArrayList<>(columnValueJsonArray.size());
        for (Object o : columnValueJsonArray) {
            if (o == null) {
                continue;
            }
            result.add(deSerializationAsEntryAsColumnValue((JSONObject) o));
        }
        return result;
    }

    private static LogMinerColumnValue deSerializationAsEntryAsColumnValue(JSONObject columnValueJsonObject) {
        if (columnValueJsonObject == null) {
            return null;
        }
        final LogMinerColumnValueImpl logMinerColumnValue = new LogMinerColumnValueImpl(
                columnValueJsonObject.getString("columnName"), columnValueJsonObject.getIntValue("columnType"));
        logMinerColumnValue.setColumnData(columnValueJsonObject.get("columnData"));
        return logMinerColumnValue;
    }

    private static Envelope.Operation deSerializationAsEntryAsCommandType(String commandTypeCode) {
        if (commandTypeCode == null) {
            return null;
        }
        return Envelope.Operation.forCode(commandTypeCode);
    }

    private static Scn deSerializationAsScn(JSONObject scnJsonObject) {
        if (scnJsonObject == null) {
            return null;
        }
        final BigInteger scn = scnJsonObject.getBigInteger("scn");
        return scn == null ? Scn.NULL : new Scn(scn);
    }

    private static Map<String, Object> serializationAsTableId(TableId tableId) {
        if (tableId == null) {
            return null;
        }
        final Map<String, Object> tableIdMap = new HashMap<>(4);
        tableIdMap.put("catalogName", tableId.catalog());
        tableIdMap.put("schemaName", tableId.schema());
        tableIdMap.put("tableName", tableId.table());
        tableIdMap.put("id", tableId.identifier());
        return tableIdMap;
    }

    private static Map<String, Object> serializationAsEntry(LogMinerDmlEntry entry) {
        if (entry == null) {
            return null;
        }
        final Envelope.Operation commandType = entry.getCommandType();
        final Map<String, Object> entryMap = new HashMap<>(9);
        entryMap.put("commandType", commandType == null ? null : commandType.code());
        entryMap.put("newLmColumnValues", serializationAsEntryAsColumnValueList(entry.getNewValues()));
        entryMap.put("oldLmColumnValues", serializationAsEntryAsColumnValueList(entry.getOldValues()));
        entryMap.put("objectOwner", entry.getObjectOwner());
        entryMap.put("objectName", entry.getObjectName());
        entryMap.put("sourceTime", entry.getSourceTime() == null ? null : entry.getSourceTime().getTime());
        entryMap.put("transactionId", entry.getTransactionId());
        entryMap.put("scn", serializationAsScn(entry.getScn()));
        entryMap.put("rowId", entry.getRowId());
        return entryMap;
    }

    private static Map<String, Object> serializationAsScn(Scn scn) {
        if (scn == null) {
            return null;
        }
        final Map<String, Object> scnMap = new HashMap<>(1);
        if (scn.isNull()) {
            scnMap.put("scn", null);
            return scnMap;
        }
        scnMap.put("scn", scn.longValue());
        return scnMap;
    }

    private static List<Map<String, Object>> serializationAsEntryAsColumnValueList(List<LogMinerColumnValue> columnValues) {
        if (columnValues == null) {
            return null;
        }
        if (columnValues.isEmpty()) {
            return new ArrayList<>(0);
        }
        return columnValues.stream().filter(Objects::nonNull)
                .map(DmlEventJsonUtil::serializationAsEntryAsColumnValue).collect(Collectors.toList());
    }

    private static Map<String, Object> serializationAsEntryAsColumnValue(LogMinerColumnValue columnValue) {
        final Map<String, Object> map = new HashMap<>(3);
        map.put("columnName", columnValue.getColumnName());
        map.put("columnData", columnValue.getColumnData());
        map.put("columnType", columnValue.getColumnType());
        return map;
    }

}
