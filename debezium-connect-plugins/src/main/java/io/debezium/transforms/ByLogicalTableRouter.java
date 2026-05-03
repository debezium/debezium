/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use {@link ToLogicalTopicRouter} instead.
 * <p>
 * This class is deprecated and maintained only for backward compatibility.
 * {@link ToLogicalTopicRouter} provides the same functionality with improved naming
 * that uses "logical destination" and "source" terminology instead of table-centric naming.
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author David Leibovic
 * @author Mario Mueller
 */
@Deprecated
public class ByLogicalTableRouter<R extends ConnectRecord<R>> extends ToLogicalTopicRouter<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByLogicalTableRouter.class);

    @Override
    public void configure(Map<String, ?> props) {
        LOGGER.warn("{} is deprecated and will be removed in a future release. Please adjust the connector configuration to use {} instead.",
                ByLogicalTableRouter.class.getName(),
                ToLogicalTopicRouter.class.getName());
        super.configure(props);
    }
}
