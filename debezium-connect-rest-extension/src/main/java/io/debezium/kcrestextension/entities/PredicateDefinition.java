/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.kcrestextension.entities;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * JSON model that describes a Predicate entry used to conditionally enable/disable/apply Single Message Transforms / SMTs.
 */
public class PredicateDefinition extends PluginDefinition {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredicateDefinition.class);

    @JsonCreator
    public PredicateDefinition(String className, ConfigDef config) {
        super(className, config);
    }

    @JsonCreator
    public static PredicateDefinition fromPluginDesc(PluginDesc<Predicate<?>> predicate) {
        String className = predicate.pluginClass().getName();
        LOGGER.info("Loading config for PREDICATE: " + className + "...");
        try {
            return new PredicateDefinition(className, predicate.pluginClass().getDeclaredConstructor().newInstance().config());
        }
        catch (ReflectiveOperationException e) {
            LOGGER.error("Unable to load PREDICATE: " + className + "\n\t Reason: " + e);
            return null;
        }
    }

    @JsonProperty("predicate")
    public String className() {
        return this.className;
    }

}
