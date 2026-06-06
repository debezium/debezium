/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schema;

import java.util.Properties;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * Implement a regex expression strategy to determine data event topic names using {@link DataCollectionId#databaseParts()}.
 *
 * @author Harvey Yue
 */
@Incubating
public class DefaultRegexTopicNamingStrategy extends AbstractRegexTopicNamingStrategy {

    public DefaultRegexTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public String getOriginTopic(DataCollectionId id) {
        return mkString(Collect.arrayListOf(prefix, id.databaseParts()), delimiter);
    }
}
