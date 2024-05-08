/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension.entities;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON model that describes a Single Message Transform (SMT) entry.
 */
public class TransformDefinition extends PluginDefinition {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformDefinition.class);

    @JsonCreator
    public TransformDefinition(String className, ConfigDef config) {
        super(className, config);
    }

    @JsonCreator
    public static TransformDefinition fromPluginDesc(PluginDesc<Transformation<?>> transformPlugin) {
        String className = transformPlugin.pluginClass().getName();
        if (className.endsWith("$Value")) {
            return null;
        }
        if (className.endsWith("$Key")) {
            className = className.substring(0, className.length() - 4);
        }
        LOGGER.info("Loading config for TRANSFORM: " + className + "...");
        try {
            return new TransformDefinition(className, transformPlugin.pluginClass().getDeclaredConstructor().newInstance().config());
        }
        catch (ReflectiveOperationException e) {
            LOGGER.error("Unable to load TRANSFORM: " + className + "\n\t Reason: " + e);
            return null;
        }
    }

    @JsonProperty("transform")
    public String className() {
        return this.className;
    }

}
