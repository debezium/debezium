/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.archunit;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import java.util.List;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import io.debezium.metadata.ComponentMetadataProvider;
import io.debezium.metadata.ConfigDescriptor;
import io.debezium.spi.converter.CustomConverter;

public class DebeziumArchRules {

    private static final List<Class<?>> CONFIG_DESCRIPTOR_REQUIRED_TYPES = List.of(
            SourceConnector.class,
            SinkConnector.class,
            Transformation.class,
            Predicate.class,
            CustomConverter.class,
            Converter.class,
            HeaderConverter.class);

    @ArchTest
    static final ArchRule connectorsShouldImplementConfigDescriptor = classes()
            .that(assignableToAnyOf(CONFIG_DESCRIPTOR_REQUIRED_TYPES))
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .and().areNotAnonymousClasses()
            .and().areNotAnnotatedWith(Deprecated.class)
            .should().implement(ConfigDescriptor.class)
            .allowEmptyShould(true)
            .because("""
                    All configurable components (connectors, transforms, converters) must implement ConfigDescriptor.

                     \
                    Add 'implements ConfigDescriptor' to your class and implement getConfigFields() \
                    to return the Field.Set describing your component's configuration""");

    @ArchTest
    static final ArchRule configDescriptorsShouldBeRegisteredInMetadataProvider = classes()
            .that(assignableToAnyOf(CONFIG_DESCRIPTOR_REQUIRED_TYPES))
            .and().doNotHaveModifier(JavaModifier.ABSTRACT)
            .and().areNotAnonymousClasses()
            .and().areNotAnnotatedWith(Deprecated.class)
            .should(beReferencedByAComponentMetadataProvider())
            .allowEmptyShould(true)
            .because("""
                    All configurable components must be registered in a ComponentMetadataProvider.\s

                    Create or update the module's ComponentMetadataProvider implementation to include your component \
                    via componentMetadataFactory.createComponentMetadata(), and ensure the provider is listed in \
                    META-INF/services/io.debezium.metadata.ComponentMetadataProvider""");

    private static DescribedPredicate<JavaClass> assignableToAnyOf(List<Class<?>> types) {
        return types.stream()
                .map(JavaClass.Predicates::assignableTo)
                .reduce((a, b) -> a.or(b))
                .orElseThrow()
                .as("assignable to any of %s", types);
    }

    private static ArchCondition<JavaClass> beReferencedByAComponentMetadataProvider() {
        return new ArchCondition<>("be referenced by a ComponentMetadataProvider") {
            @Override
            public void check(JavaClass javaClass, ConditionEvents events) {
                boolean referenced = javaClass.getDirectDependenciesToSelf().stream()
                        .map(Dependency::getOriginClass)
                        .anyMatch(origin -> origin.isAssignableTo(ComponentMetadataProvider.class));

                if (!referenced) {
                    events.add(SimpleConditionEvent.violated(javaClass,
                            String.format("Class <%s> is not registered in any ComponentMetadataProvider. "
                                    + "Add it to your module's ComponentMetadataProvider.getConnectorMetadata() method "
                                    + "using componentMetadataFactory.createComponentMetadata(new %s(), ...)",
                                    javaClass.getName(), javaClass.getSimpleName())));
                }
            }
        };
    }
}
