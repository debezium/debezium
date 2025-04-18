/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
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

    private static final String POST_PROCESSOR_MISS_CONFIGURATION_ERROR_MESSAGE = "Post processor '%s' is missing '%s.type' and/or '%s.<option>' configurations";
    private final String TYPE_SUFFIX = ".type";

    @Override
    public PostProcessorRegistry createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        String postProcessorNameList = configuration.getString(CommonConnectorConfig.CUSTOM_POST_PROCESSORS);
        List<String> processorNames = Strings.listOf(postProcessorNameList, x -> x.split(","), String::trim);

        List<PostProcessor> postProcessors = processorNames.stream()
                .map(postProcessorName -> getPostProcessor(configuration, postProcessorName))
                .collect(Collectors.toList());

        return new PostProcessorRegistry(postProcessors);
    }

    private PostProcessor getPostProcessor(Configuration configuration, String postProcessorName) {

        String type = postProcessorName + TYPE_SUFFIX;
        Map<String, String> postProcessorConfigs = configuration.subset(postProcessorName, true).asMap();

        if (!configuration.hasKey(type) || postProcessorConfigs.isEmpty()) {
            throw new DebeziumException(String.format(POST_PROCESSOR_MISS_CONFIGURATION_ERROR_MESSAGE, postProcessorName,
                    postProcessorName, postProcessorName));
        }

        PostProcessor postProcessor = configuration.getInstance(type, PostProcessor.class);
        postProcessor.configure(postProcessorConfigs);
        return postProcessor;
    }

    @Override
    public Class<PostProcessorRegistry> getServiceClass() {
        return PostProcessorRegistry.class;
    }

}
