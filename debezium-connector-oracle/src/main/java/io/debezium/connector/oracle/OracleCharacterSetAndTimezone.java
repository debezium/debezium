/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

/**
 * Represents the character set and time zone of the oracle database
 *
 * @author butioy
 */
public class OracleCharacterSetAndTimezone {

    /**
     * Character set of the oracle database
     */
    private final String characterSet;

    /**
     * The time zone of the oracle database
     */
    private final String databaseTimezone;

    /**
     * the time zone of the oracle database connection session
     */
    private final String sessionTimezoneOffset;

    public OracleCharacterSetAndTimezone(String characterSet, String dbTimezone, String sessionTimezoneOffset) {
        this.characterSet = characterSet;
        this.databaseTimezone = dbTimezone;
        this.sessionTimezoneOffset = sessionTimezoneOffset;
    }

    public String getCharacterSet() {
        return characterSet;
    }

    public String getDatabaseTimezone() {
        return databaseTimezone;
    }

    public String getSessionTimezoneOffset() {
        return sessionTimezoneOffset;
    }

    @Override
    public String toString() {
        return "OracleCharacterSetAndTimezone{" +
                "characterSet='" + characterSet + '\'' +
                ", databaseTimezone='" + databaseTimezone + '\'' +
                ", sessionTimezoneOffset='" + sessionTimezoneOffset + '\'' +
                '}';
    }
}
