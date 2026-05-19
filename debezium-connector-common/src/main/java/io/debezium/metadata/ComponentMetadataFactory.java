/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.metadata;

import io.debezium.config.Field;

public class ComponentMetadataFactory {

    public ComponentMetadataFactory() {
    }

    public <T extends ConfigDescriptor> ComponentMetadata createComponentMetadata(T component, String version) {
        return createComponentMetadata(component, null, null, version);
    }

    public <T extends ConfigDescriptor> ComponentMetadata createComponentMetadata(T component, String displayName, String version) {
        return createComponentMetadata(component, displayName, null, version);
    }

    public <T extends ConfigDescriptor> ComponentMetadata createComponentMetadata(T component, String displayName, String description, String version) {
        return new ComponentMetadata() {
            @Override
            public ComponentDescriptor getComponentDescriptor() {
                if (displayName != null || description != null) {
                    return new ComponentDescriptor(component.getClass().getName(), displayName, description, version);
                }
                return new ComponentDescriptor(component.getClass().getName(), version);
            }

            @Override
            public Field.Set getComponentFields() {
                return component.getConfigFields();
            }
        };
    }
}
