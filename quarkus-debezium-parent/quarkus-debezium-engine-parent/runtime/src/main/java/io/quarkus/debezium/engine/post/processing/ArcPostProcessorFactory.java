/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.engine.post.processing;

import java.util.List;

import io.debezium.processors.spi.PostProcessor;
import io.debezium.processors.spi.PostProcessorFactory;
import io.quarkus.arc.Arc;

public class ArcPostProcessorFactory implements PostProcessorFactory {

    @Override
    public List<PostProcessor> get() {
        return Arc.container().select(PostProcessor.class)
                .stream()
                .toList();
    }
}
