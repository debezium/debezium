/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.mongodb.deployment;

import java.util.List;

import io.quarkus.debezium.mongodb.deployment.DebeziumDatasourceBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;

public class MongoDbClientProcessor {

    /**
     * Used to map package level {@link MongoConnectionNameBuildItem} to public {@link DebeziumDatasourceBuildItem}
     *
     * @param mongoConnectionNameBuildItems contains information related to mongodb client connections
     * @param datasourceBuildItemBuildProducer producer for {@link DebeziumDatasourceBuildItem}
     */
    @BuildStep
    public void mapping(List<MongoConnectionNameBuildItem> mongoConnectionNameBuildItems,
                        BuildProducer<DebeziumDatasourceBuildItem> datasourceBuildItemBuildProducer) {
        mongoConnectionNameBuildItems.forEach(item -> datasourceBuildItemBuildProducer.produce(new DebeziumDatasourceBuildItem(item.getName())));
    }
}
