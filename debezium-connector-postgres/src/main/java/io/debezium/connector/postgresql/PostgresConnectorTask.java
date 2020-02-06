/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.common.ChangeEventSourceBasedTask;
import io.debezium.pipeline.ChangeEventSourceFacade;

/**
 * Kafka connect source task which uses Postgres logical decoding over a streaming replication connection to process DB changes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class PostgresConnectorTask extends ChangeEventSourceBasedTask {

    private PostgresChangeEventSourceFacade postgresChangeEventSourceFacade;

    @Override
    protected ChangeEventSourceFacade createEventSourceFacade(Configuration config) {
        postgresChangeEventSourceFacade = new PostgresChangeEventSourceFacade(config, loader -> getPreviousOffset(loader));
        return postgresChangeEventSourceFacade;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return PostgresConnectorConfig.ALL_FIELDS;
    }

    /**
     * TODO remove this used only in test
     */
    PostgresTaskContext getTaskContext() {
        return postgresChangeEventSourceFacade.getTaskContext();
    }

}
