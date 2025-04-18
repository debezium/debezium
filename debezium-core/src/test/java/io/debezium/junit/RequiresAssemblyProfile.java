/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation used to group tests that require the assembly profile, using the {@code isAssemblyProfileActive} system property.
 *
 * @author René Kerner
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface RequiresAssemblyProfile {

    String REQUIRES_ASSEMBLY_PROFILE_PROPERTY = "isAssemblyProfileActive";

    /**
     * The optional reason why the test is skipped.
     * @return the reason why the test is skipped
     */
    String value() default "Maven 'assembly' profile required (use '-Passembly' to enable assembly profile)";
}
