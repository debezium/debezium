/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import io.debezium.config.EnumeratedValue;

import oracle.streams.XStreamUtility;

/**
 * This enum class includes Oracle 11 and 12 major versions
 * It returns position version based on the Oracle release.
 * Position version get used in conversion SCN (system change number) into position
 */
public enum OracleVersion implements EnumeratedValue {

    V11("11"),
    V12Plus("12+");

    private final String version;

    OracleVersion(String version) {
        this.version = version;
    }

    @Override
    public String getValue() {
        return version;
    }

    public int getPosVersion() {
        switch (version) {
            case "11":
                return XStreamUtility.POS_VERSION_V1;
            case "12+":
                return XStreamUtility.POS_VERSION_V2;
            default:
                return XStreamUtility.POS_VERSION_V2;
        }
    }

    public static OracleVersion parse(String value) {
        if (value == null) {
            return null;
        }
        value = value.trim();

        for (OracleVersion option : OracleVersion.values()) {
            if (option.getValue().equalsIgnoreCase(value)) {
                return option;
            }
        }

        return null;
    }

    public static OracleVersion parse(String value, String defaultValue) {
        OracleVersion option = parse(value);

        if (option == null && defaultValue != null) {
            option = parse(defaultValue);
        }

        return option;
    }
}
