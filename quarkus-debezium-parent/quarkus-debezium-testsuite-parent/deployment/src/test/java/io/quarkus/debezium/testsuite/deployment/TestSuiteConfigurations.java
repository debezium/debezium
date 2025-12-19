/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.testsuite.deployment;

public class TestSuiteConfigurations {

    public static final int TIMEOUT = Integer.parseInt(System.getProperty("quarkus.testsuite.timeout", "100"));
}
