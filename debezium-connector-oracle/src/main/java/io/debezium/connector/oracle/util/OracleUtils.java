/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import io.debezium.util.Strings;

/**
 * Oracle-specific utility methods.
 *
 * @author Chris Cranford
 */
public class OracleUtils {

    /**
     * Get the object name using Oracle case-semantics. If the name is quoted, its case is left as is,
     * but if it isn't quoted, the case is automatically converted to upper-case.
     *
     * @param objectName the object name
     * @return the object name with case-semantics applied
     */
    public static String getObjectName(String objectName) {
        if (!Strings.isNullOrEmpty(objectName)) {
            if (objectName.startsWith("\"") && objectName.endsWith("\"") && objectName.length() > 2) {
                return objectName.substring(1, objectName.length() - 1);
            }
            return objectName.toUpperCase();
        }
        return objectName;
    }

    private OracleUtils() {
    }
}
