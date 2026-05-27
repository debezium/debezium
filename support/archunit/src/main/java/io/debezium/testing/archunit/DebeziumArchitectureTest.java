/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.archunit;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchTests;

@AnalyzeClasses(packages = "io.debezium", importOptions = { ImportOption.DoNotIncludeTests.class, DoNotIncludeTestJars.class })
public class DebeziumArchitectureTest {

    @ArchTest
    static final ArchTests rules = ArchTests.in(DebeziumArchRules.class);
}
