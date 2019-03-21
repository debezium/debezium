/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql.junit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.junit.AnnotationBasedTestRule;
import io.debezium.util.Strings;

/**
 * JUnit rule that skips a test based on the {@link SkipForLegacyParser} annotation on either a test method or a test class.
 */
public class SkipTestForLegacyParser extends AnnotationBasedTestRule {
    private static boolean isLegacyParserInUse() {
        final String mode = System.getProperty("ddl.parser.mode");
        return !Strings.isNullOrEmpty(mode) && mode.equalsIgnoreCase(MySqlConnectorConfig.DdlParsingMode.LEGACY.getValue());
    }

    @Override
    public Statement apply(Statement base, Description description) {
        final SkipForLegacyParser skipHasName = hasAnnotation(description, SkipForLegacyParser.class);
        if (skipHasName != null && isLegacyParserInUse()) {
            String reasonForSkipping = "Legacy parser is used";
            return emptyStatement(reasonForSkipping, description);
        }
        return base;
    }
}
