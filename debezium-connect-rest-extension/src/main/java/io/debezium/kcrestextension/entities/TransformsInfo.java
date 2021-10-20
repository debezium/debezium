/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
public class TransformsInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransformsInfo.class);

    private final String className;
    private final Map<String, PropertyDescriptor> properties;

    @JsonCreator
    public TransformsInfo(String className, ConfigDef config) {
        this.className = className;
        this.properties = getConfigProperties(className, config);
    }

    @JsonCreator
    public TransformsInfo(String className, Class<? extends Transformation<?>> transformationClass) {
        this.className = className;
        try {
            LOGGER.info("Loading config for TRANSFORM: " + className + "...");
            this.properties = getConfigProperties(transformationClass.getName(), transformationClass.newInstance().config());
        }
        catch (InstantiationException | IllegalAccessException e) {
            LOGGER.error("Unable to load TRANSFORM: " + className
                    + "\n\t Reason: " + e.toString());
            throw new RuntimeException(e);
        }
    }

    private static Map<String, PropertyDescriptor> getConfigProperties(String className, ConfigDef configDef) {
        Map<String, PropertyDescriptor> configProperties = new HashMap<>();
        configDef.configKeys().forEach((fieldName, configKey) -> {
            if (null != configKey.documentation
                    && !configKey.documentation.startsWith("Deprecated")
                    && !configKey.internalConfig) {
                configProperties.put(fieldName, new PropertyDescriptor(className, configKey));
            }
        });
        return configProperties;
    }

    public TransformsInfo(PluginDesc<Transformation<?>> transform) {
        this(transform.className(), transform.pluginClass());
    }

    @JsonProperty("transform")
    public String className() {
        return this.className;
    }

    @JsonProperty
    public Map<String, PropertyDescriptor> properties() {
        return this.properties;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o != null && this.getClass() == o.getClass()) {
            TransformsInfo that = (TransformsInfo) o;
            return Objects.equals(this.className, that.className)
                    && Objects.equals(this.properties, that.properties);
        }
        else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.className, this.properties);
    }

    public String toString() {
        return "ConnectorPluginInfo{" + "className='" + this.className + '\'' +
                ", documentation='" + this.properties + '\'' +
                '}';
    }
}
