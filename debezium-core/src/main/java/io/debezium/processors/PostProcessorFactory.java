/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.processors;

import java.util.List;

import io.debezium.processors.spi.PostProcessor;

public interface PostProcessorFactory {

    List<PostProcessor> get();
}
