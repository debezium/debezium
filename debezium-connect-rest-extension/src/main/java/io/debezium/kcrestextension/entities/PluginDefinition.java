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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base class for JSON models that describes a Single Message Transform (SMT) entry or a Kafka Connect Predicate entry.
 */
abstract class PluginDefinition {

    protected final String className;
    protected final Map<String, PropertyDescriptor> properties;

    PluginDefinition(String className, ConfigDef config) {
        this.className = className;
        this.properties = getConfigProperties(className, config);
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

    @JsonProperty
    public Map<String, PropertyDescriptor> properties() {
        return this.properties;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        else if (o != null && this.getClass() == o.getClass()) {
            PluginDefinition that = (PluginDefinition) o;
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
        return "PluginDefinition{" + "className='" + this.className + '\'' +
                ", documentation='" + this.properties + '\'' +
                '}';
    }
}
