/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use {@link ToLogicalTopicRouter} instead.
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
        super.configure(applyLegacyConfigurationIfApplicable(props));
    }

    /**
     * This allows older configurations to remain usable as long as the configuration continues to reference
     * this class rather than the new {@link ToLogicalTopicRouter} class name.
     * <ul>
     *     <li>The {@code key.field.name} defaults to {@code __dbz__physicalTableIdentifier}.</li>
     *     <li>The {@code logical.table.cache.size} property is recognized and mapped to {@code logical.destination.cache.size} if absent.</li>
     * </ul>
     *
     * @param props the supplied configuration, should not be {@code null}
     * @return the adjusted configuration, if applicable, never {@code null}
     */
    private Map<String, ?> applyLegacyConfigurationIfApplicable(Map<String, ?> props) {
        // Handles legacy setups when using the deprecated implementation
        final Map<String, Object> adjustedProps = new HashMap<>(props);
        adjustedProps.putIfAbsent("key.field.name", "__dbz__physicalTableIdentifier");

        if (adjustedProps.containsKey("logical.table.cache.size") && !adjustedProps.containsKey("logical.destination.cache.size")) {
            adjustedProps.put("logical.destination.cache.size", adjustedProps.get("logical.table.cache.size"));
        }

        return adjustedProps;
    }
}