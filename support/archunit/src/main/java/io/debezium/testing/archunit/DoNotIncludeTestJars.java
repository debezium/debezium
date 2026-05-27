/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.archunit;

import java.util.regex.Pattern;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;

public class DoNotIncludeTestJars implements ImportOption {

    private static final Pattern TEST_JAR_PATTERN = Pattern.compile(".*-tests\\.jar.*");

    @Override
    public boolean includes(Location location) {
        return !location.matches(TEST_JAR_PATTERN);
    }
}
