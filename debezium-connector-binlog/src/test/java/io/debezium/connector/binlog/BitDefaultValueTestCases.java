/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class BitDefaultValueTestCases {

    private BitDefaultValueTestCases() {
    }

    public static List<BitDefaultValueCase> readCases() {
        final var resource = Objects.requireNonNull(
                BitDefaultValueTestCases.class.getResourceAsStream("/data/bit-default-values.csv"));
        try (var reader = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))) {
            return reader.lines().skip(1).map(BitDefaultValueTestCases::parseCase).toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Stream<BitDefaultValueCase> cases() {
        return readCases().stream();
    }

    public static Stream<BitDefaultValueCase> mysqlNonStrictCases() {
        // MySQL clamps these defaults to the unsigned 64-bit maximum in non-strict mode.
        final String maximum = "18446744073709551615";
        return List.of(
                new BitDefaultValueCase("non_strict_w64_hex_overflow", 64, "0x10000000000000000", "modify-column", maximum, false),
                new BitDefaultValueCase("non_strict_w64_hex_leading_zero", 64, "0x000000000000000001", "modify-column", maximum, false),
                new BitDefaultValueCase("non_strict_w64_binary_leading_zero", 64, "b'" + "0".repeat(Long.SIZE) + "1'", "modify-column", maximum, false),
                new BitDefaultValueCase("non_strict_w64_binary_overflow", 64, "0b1" + "0".repeat(Long.SIZE), "modify-column", maximum, false),
                new BitDefaultValueCase("non_strict_w64_quotedhex_leading_zero", 64, "X'000000000000000001'", "modify-column", maximum, false))
                .stream();
    }

    public static Stream<BitDefaultValueCase> mariaDbNonStrictCases() {
        // MariaDB accepts the significant value of quoted hex strings with leading zero bytes.
        return Stream.of(new BitDefaultValueCase("w64_quotedhex_leading_zero", 64, "X'000000000000000001'", "modify-column", "1", false));
    }

    private static BitDefaultValueCase parseCase(String line) {
        final String[] values = line.split(",", -1);
        if (values.length != 6) {
            throw new IllegalArgumentException("Expected six comma-separated fields: " + line);
        }
        return new BitDefaultValueCase(values[0], Integer.parseInt(values[1]), values[2], values[3], values[4], Boolean.parseBoolean(values[5]));
    }

    public record BitDefaultValueCase(String id, int width, String literal, String action, String expectedValue, boolean nullable) {
        public String initialDefaultLiteral() {
            return "NULL".equals(expectedValue) || "0".equals(expectedValue) ? "b'1'" : "b'0'";
        }

        @Override
        public String toString() {
            return id;
        }
    }
}
