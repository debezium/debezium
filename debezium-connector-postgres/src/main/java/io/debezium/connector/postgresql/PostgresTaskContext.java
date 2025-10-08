/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading various configuration options
 * and creating other objects with these various options.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {

    protected PostgresTaskContext(PostgresConnectorConfig config, PostgresSchema schema) {
        super(config, config.getCustomMetricTags());
    }
}
