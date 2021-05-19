/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.document.Document;

/**
 * Abstract implementation of the {@link StreamingAdapter} for which all streaming adapters are derived.
 *
 * @author Chris Cranford
 */
public abstract class AbstractStreamingAdapter implements StreamingAdapter {

    protected final OracleConnectorConfig connectorConfig;

    public AbstractStreamingAdapter(OracleConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    protected Scn resolveScn(Document document) {
        final String scn = document.getString(SourceInfo.SCN_KEY);
        if (scn == null) {
            Long scnValue = document.getLong(SourceInfo.SCN_KEY);
            return Scn.valueOf(scnValue == null ? 0 : scnValue);
        }
        return Scn.valueOf(scn);
    }
}
