/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.processors;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.processors.spi.PostProcessor;
import io.debezium.processors.spi.PostProcessorFactory;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.util.Strings;

/**
 * An implementation of the {@link ServiceProvider} contract for the {@link PostProcessorRegistry}.
 *
 * @author Chris Cranford
 */
public class PostProcessorRegistryServiceProvider implements ServiceProvider<PostProcessorRegistry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessorRegistryServiceProvider.class);

    private static final String POST_PROCESSOR_MISS_CONFIGURATION_ERROR_MESSAGE = "Post processor '%s' is missing '%s.type' and/or '%s.<option>' configurations";
    private final String TYPE_SUFFIX = ".type";
    private final ServiceLoader<PostProcessorFactory> postProcessorFactory = ServiceLoader.load(PostProcessorFactory.class);

    @Override
    public PostProcessorRegistry createService(Configuration configuration, ServiceRegistry serviceRegistry) {
        String postProcessorNameList = configuration.getString(CommonConnectorConfig.CUSTOM_POST_PROCESSORS);
        List<PostProcessor> postProcessors = getPostProcessors(configuration, postProcessorNameList);

        return new PostProcessorRegistry(postProcessors);
    }

    private List<PostProcessor> getPostProcessors(Configuration configuration, String postProcessorNameList) {
        List<String> processorNames = Strings.listOf(postProcessorNameList, x -> x.split(","), String::trim);

        List<PostProcessor> postProcessors = processorNames
                .stream()
                .map(postProcessorName -> getPostProcessor(configuration, postProcessorName))
                .collect(Collectors.toList());

        List<PostProcessor> externalPostProcessors = postProcessorFactory
                .findFirst()
                .map(PostProcessorFactory::get)
                .orElse(Collections.emptyList());

        postProcessors.addAll(externalPostProcessors);

        return postProcessors;
    }

    private PostProcessor getPostProcessor(Configuration configuration, String postProcessorName) {

        String type = postProcessorName + TYPE_SUFFIX;

        Map<String, String> postProcessorConfigsOldConvention = configuration.subset(postProcessorName, true).asMap();
        Map<String, String> postProcessorConfigsNewConvention = configuration.subset(CommonConnectorConfig.CUSTOM_POST_PROCESSORS.name() + "." + postProcessorName, true)
                .asMap();

        if (!postProcessorConfigsNewConvention.isEmpty()) {
            PostProcessor postProcessor = configuration.getInstance(CommonConnectorConfig.CUSTOM_POST_PROCESSORS.name() + "." + type, PostProcessor.class);
            postProcessor.configure(postProcessorConfigsNewConvention);
            return postProcessor;
        }

        if (!configuration.hasKey(type) || postProcessorConfigsOldConvention.isEmpty()) {
            throw new DebeziumException(String.format(POST_PROCESSOR_MISS_CONFIGURATION_ERROR_MESSAGE, postProcessorName,
                    postProcessorName, postProcessorName));
        }

        LOGGER.warn("The configuration used for post-processors is deprecated, please refer to the documentation.");

        PostProcessor postProcessor = configuration.getInstance(type, PostProcessor.class);
        postProcessor.configure(postProcessorConfigsOldConvention);
        return postProcessor;
    }

    @Override
    public Class<PostProcessorRegistry> getServiceClass() {
        return PostProcessorRegistry.class;
    }

}
