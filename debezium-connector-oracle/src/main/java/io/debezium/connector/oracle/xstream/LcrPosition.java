/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.xstream;

import java.sql.SQLException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.Scn;
import io.debezium.util.HexConverter;
import io.debezium.util.Strings;

import oracle.streams.StreamsException;
import oracle.streams.XStreamUtility;

/**
 * The logical encapsulation of raw LCR byte array.
 *
 * @author Jiri Pechanec
 */
public class LcrPosition implements Comparable<LcrPosition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LcrPosition.class);

    private final byte[] rawPosition;
    private final String stringFromat;
    private final Scn scn;
    private final Scn commitScn;

    public LcrPosition(byte[] rawPosition) {
        this.rawPosition = rawPosition;
        this.stringFromat = HexConverter.convertToHexString(rawPosition);
        try {
            scn = new Scn(XStreamUtility.getSCNFromPosition(rawPosition).bigIntegerValue());
            commitScn = new Scn(XStreamUtility.getCommitSCNFromPosition(rawPosition).bigIntegerValue());
        }
        catch (SQLException | StreamsException e) {
            throw new RuntimeException(e);
        }
        LOGGER.trace("LCR position {} converted to SCN {}", rawPosition, stringFromat, scn);
    }

    public static LcrPosition valueOf(String rawPosition) {
        if (rawPosition == null) {
            return null;
        }
        return new LcrPosition(Strings.hexStringToByteArray(rawPosition));
    }

    public byte[] getRawPosition() {
        return rawPosition;
    }

    public Scn getScn() {
        return scn;
    }

    public Scn getCommitScn() {
        return commitScn;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(rawPosition);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LcrPosition other = (LcrPosition) obj;
        return Arrays.equals(rawPosition, other.rawPosition);
    }

    @Override
    public String toString() {
        return stringFromat;
    }

    @Override
    public int compareTo(LcrPosition o) {
        if (o == null) {
            return 1;
        }
        // This should not be necessary but we are providing it as defensive programming technique
        final int lenDiff = rawPosition.length - o.rawPosition.length;
        if (lenDiff != 0) {
            return lenDiff;
        }

        for (int i = 0; i < rawPosition.length; i++) {
            int diff = (rawPosition[i] & 0xFF) - (o.rawPosition[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }

        return 0;
    }
}
