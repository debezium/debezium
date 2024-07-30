/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.common.CdcSourceTaskContext;

public class OracleTaskContext extends CdcSourceTaskContext {

    public OracleTaskContext(OracleConnectorConfig config, OracleDatabaseSchema schema) {
        super(config, config.getCustomMetricTags(), schema::tableIds);
    }
}
