/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * @author Chris Cranford
 */
public class OracleCloudEventsMaker extends CloudEventsMaker {

    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";

    static final Set<String> ORACLE_SOURCE_FIELDS = Collect.unmodifiableSet(
            SCN_KEY,
            COMMIT_SCN_KEY,
            LCR_POSITION_KEY);

    public OracleCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                  String cloudEventsSchemaName) {
        super(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";scn:" + sourceField(SCN_KEY)
                + ";commit_scn:" + sourceField(COMMIT_SCN_KEY)
                + ";lcr_position:" + sourceField(LCR_POSITION_KEY);
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return ORACLE_SOURCE_FIELDS;
    }
}
