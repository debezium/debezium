/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.relational;

import io.debezium.document.Document;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalMetadataFactory;
import io.debezium.relational.history.JsonTableChangeSerializer;

/**
 * Writes and reads the Oracle-specific column metadata in schema history, nested under an {@code oracle}
 * entry of each column document. Documents written before that entry existed are read with the metadata
 * left unset.
 *
 * @author Chris Cranford
 */
public class OracleJsonTableChangeSerializer extends JsonTableChangeSerializer {

    static final String ORACLE_KEY = "oracle";
    static final String OCTET_LENGTH_KEY = "octetLength";

    public OracleJsonTableChangeSerializer(RelationalMetadataFactory metadataFactory) {
        super(metadataFactory);
    }

    @Override
    protected void writeColumnExtensions(Column column, Document document) {
        if (column instanceof OracleColumn oracleColumn && oracleColumn.octetLength() != Column.UNSET_INT_VALUE) {
            document.setDocument(ORACLE_KEY).setNumber(OCTET_LENGTH_KEY, oracleColumn.octetLength());
        }
    }

    @Override
    protected void readColumnExtensions(Document document, ColumnEditor editor) {
        final Document oracle = document.getDocument(ORACLE_KEY);
        if (oracle != null && editor instanceof OracleColumnEditor oracleEditor) {
            final Integer octetLength = oracle.getInteger(OCTET_LENGTH_KEY);
            if (octetLength != null) {
                oracleEditor.octetLength(octetLength);
            }
        }
    }
}
