/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.transforms.reselect;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.oracle.SourceInfo;
import io.debezium.relational.TableId;
import io.debezium.transforms.reselect.DefaultReselectColumnsMetadataProvider;
import io.debezium.util.Strings;

/**
 * An Oracle extension to the {@link DefaultReselectColumnsMetadataProvider}.
 *
 * This implementation provides specialized handling for {@code CLOB}, {@code NCLOB}, and {@code BLOB} columns
 * as well as providing a Flashback Query rather than using the current row's state.
 *
 * @author Chris Cranford
 */
@Incubating
public class OracleReselectColumnsMetadataProvider<R extends ConnectRecord<R>> extends DefaultReselectColumnsMetadataProvider<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleReselectColumnsMetadataProvider.class);

    @Override
    public String getName() {
        return "oracle";
    }

    @Override
    public String getQuery(R record, Struct source, List<String> columns, TableId tableId) {
        final String commitScn = source.getString(SourceInfo.COMMIT_SCN_KEY);
        if (Strings.isNullOrBlank(commitScn)) {
            // Use default implementation
            return super.getQuery(record, source, columns, tableId);
        }
        return getFlashbackQuery(record, columns, tableId, commitScn);
    }

    @Override
    public Object convertValue(int jdbcType, Object value) {
        try {
            switch (jdbcType) {
                case Types.CLOB:
                case Types.NCLOB:
                    if (value instanceof Clob) {
                        return ((Clob) value).getSubString(1, (int) ((Clob) value).length());
                    }
                    break;
                case Types.BLOB:
                    if (value instanceof Blob) {
                        return ByteBuffer.wrap(((Blob) value).getBytes(1, (int) ((Blob) value).length()));
                    }
            }
        }
        catch (Exception e) {
            LOGGER.warn("Failed to convert value", e);
        }
        return value;
    }

    private String getFlashbackQuery(R record, List<String> columns, TableId tableId, String commitScn) {
        final String tableName = String.format("%s.%s", tableId.schema(), tableId.table());
        final StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(String.join(",", columns));
        query.append(" FROM ");
        query.append("(SELECT * FROM ").append(tableName).append(" AS OF SCN ").append(commitScn).append(")");
        query.append(" WHERE ").append(createPrimaryKeyWhereClause(record));
        return query.toString();
    }

}
