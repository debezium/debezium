/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * @author Chris Cranford
 */
public class OracleRecordParser extends RecordParser {

    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";

    static final Set<String> ORACLE_SOURCE_FIELD = Collect.unmodifiableSet(
            SCN_KEY,
            COMMIT_SCN_KEY,
            LCR_POSITION_KEY);

    public OracleRecordParser(RecordAndMetadata recordAndMetadata) {
        super(recordAndMetadata, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (ORACLE_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from Oracle connector");
    }
}
