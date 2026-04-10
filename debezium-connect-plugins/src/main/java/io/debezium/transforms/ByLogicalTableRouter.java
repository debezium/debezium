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
 * A logical table consists of one or more physical tables with the same schema. A common use case is sharding -- the
 * two physical tables `db_shard1.my_table` and `db_shard2.my_table` together form one logical table.
 * <p>
 * This Transformation allows us to change a record's topic name and send change events from multiple physical tables to
 * one topic. For instance, we might choose to send the two tables from the above example to the topic
 * `db_shard.my_table`. The config options {@link #TOPIC_REGEX} and {@link #TOPIC_REPLACEMENT} are used
 * to change the record's topic.
 * <p>
 * Now that multiple physical tables can share a topic, the event's key may need to be augmented to include fields other
 * than just those for the record's primary/unique key, since these are not guaranteed to be unique across tables. We
 * need some identifier added to the key that distinguishes the different physical tables. The field name specified by
 * the config option {@link #KEY_FIELD_NAME} is added to the key schema for this purpose. By default, its value will
 * be the old topic name, but if a custom value is desired, the config options {@link #KEY_FIELD_REGEX} and
 * {@link #KEY_FIELD_REPLACEMENT} may be used to change it. For instance, in our above example, we might choose to
 * make the identifier `db_shard1` and `db_shard2` respectively.
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
