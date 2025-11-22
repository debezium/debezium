/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.config;

import java.util.regex.Pattern;

public final class CommonConfigurationPatterns {

    public static final Pattern PASSWORD_PATTERN = Pattern.compile(".*secret$|.*password$|.*sasl\\.jaas\\.config$|.*basic\\.auth\\.user\\.info|.*registry\\.auth\\.client-secret",
            Pattern.CASE_INSENSITIVE);
}
