/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * A base class for all connector classes in Debezium. It initializes and validates
 * common parameters and attributes.
 *
 * @author Jiri Pechanec
 */
public abstract class BaseConnector extends SourceConnector {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        // Validate the configuration ...
        final Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(CommonConnectorConfig.CONNECT_FIELDS, logger::error)) {
            throw new ConnectException("Error configuring a connector; check the logs for details");
        }
    }
}
