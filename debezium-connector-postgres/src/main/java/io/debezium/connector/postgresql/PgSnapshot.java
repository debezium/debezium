/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;

/**
 * This class contains the information returned by the pg_current_snapshot function.
 *
 * @author Mario Fiore Vitale
 */
public class PgSnapshot {

    private static final String SNAPSHOT_FORMAT = "(\\d+):(\\d+):((\\d+,*)+)*";
    private static final Pattern SNAPSHOT_PATTERN = Pattern.compile(SNAPSHOT_FORMAT);
    private static final String SEPARATOR = ",";

    private final Long xMin;
    private final Long xMax;
    private final Set<Long> xip;

    public PgSnapshot(Long xMin, Long xMax, Set<Long> xip) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.xip = xip;
    }

    public Long getXMin() {
        return xMin;
    }

    public Long getXMax() {
        return xMax;
    }

    public Set<Long> getXip() {
        return xip;
    }

    /**
     * Returns a PgSnapshot instance representing the specified snapshot string
     *
     * @param snapshotString is the string returned by the pg_current_snapshot function
     * @return a PgSnapshot representing the {@code snapshotString}
     */
    public static PgSnapshot valueOf(String snapshotString) {

        Matcher matcher = SNAPSHOT_PATTERN.matcher(snapshotString);

        if (matcher.matches()) {

            Long xmin = Long.parseLong(matcher.group(1));
            Long xmax = Long.parseLong(matcher.group(2));

            Set<Long> xip = Set.of();
            if (matcher.group(3) != null) {
                xip = Arrays.stream(matcher.group(3).split(SEPARATOR))
                        .map(Long::parseLong)
                        .collect(Collectors.toSet());
            }

            return new PgSnapshot(xmin, xmax, xip);
        }

        throw new DebeziumException(String.format("Unable to parse PgCurrentSnapshot result %s.", snapshotString));
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PgSnapshot that = (PgSnapshot) o;
        return Objects.equals(xMin, that.xMin) && Objects.equals(xMax, that.xMax) && Objects.equals(xip, that.xip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(xMin, xMax, xip);
    }

    @Override
    public String toString() {
        return "PgSnapshot{" +
                "xMin=" + xMin +
                ", xMax=" + xMax +
                ", xip=" + xip +
                '}';
    }
}
