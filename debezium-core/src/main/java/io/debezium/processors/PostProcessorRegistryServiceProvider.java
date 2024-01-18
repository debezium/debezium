/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import java.util.List;
import java.util.stream.Collectors;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.util.Strings;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link PostProcessorRegistry}.
 *
 * @author Chris Cranford
 */
public class PostProcessorRegistryServiceProvider implements ServiceProvider<PostProcessorRegistry> {

    private final String TYPE_SUFFIX = ".type";

    @Override
    public PostProcessorRegistry createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        String postProcessorNameList = configuration.getString(CommonConnectorConfig.CUSTOM_POST_PROCESSORS);
        List<String> processorClassNames = Strings.listOf(postProcessorNameList, x -> x.split(","), String::trim);

        List<PostProcessor> postProcessors = processorClassNames.stream()
                .map(className -> {
                    String type = className + TYPE_SUFFIX;
                    PostProcessor postProcessor = configuration.getInstance(type, PostProcessor.class);
                    postProcessor.configure(configuration.subset(className, true).asMap());
                    return postProcessor;
                }).collect(Collectors.toList());

        return new PostProcessorRegistry(postProcessors);
    }

    @Override
    public Class<PostProcessorRegistry> getServiceClass() {
        return PostProcessorRegistry.class;
    }

}
