/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;

import org.bson.BsonTimestamp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class BsonTimestampParserTest {

    @ParameterizedTest
    @MethodSource("validTimestamps")
    void shouldParseTimestamp(String value, long seconds, long increment) {
        assertThat(BsonTimestampParser.parse(value)).isEqualTo(new BsonTimestamp((int) seconds, (int) increment));
    }

    static Stream<Arguments> validTimestamps() {
        return Stream.of(
                Arguments.of("0", 0L, 0L),
                Arguments.of("30", 30L, 0L),
                Arguments.of(" 30 ", 30L, 0L),
                Arguments.of("1789084800", 1789084800L, 0L),
                Arguments.of("2147483647", 2147483647L, 0L),
                Arguments.of("2147483648", 2147483648L, 0L),
                Arguments.of("4294967295", 4294967295L, 0L),
                Arguments.of("1970-01-01T00:00:00Z", 0L, 0L),
                Arguments.of("1970-01-01T00:00:30.000Z", 30L, 0L),
                Arguments.of("2026-09-11T00:00:00Z", 1789084800L, 0L),
                Arguments.of("2026-09-11T09:00:00+09:00", 1789084800L, 0L),
                Arguments.of("2026-09-10T19:00:00-05:00", 1789084800L, 0L),
                Arguments.of("2038-01-19T03:14:08Z", 2147483648L, 0L),
                Arguments.of("2106-02-07T06:28:15Z", 4294967295L, 0L),
                Arguments.of("{\"$timestamp\":{\"t\":30,\"i\":7}}", 30L, 7L),
                Arguments.of(" {\"$timestamp\": {\"i\": 7, \"t\": 30}} ", 30L, 7L),
                Arguments.of("{\"$timestamp\":{\"t\":0,\"i\":0}}", 0L, 0L),
                Arguments.of("{\"$timestamp\":{\"t\":2147483648,\"i\":2147483648}}", 2147483648L, 2147483648L),
                Arguments.of("{\"$timestamp\":{\"t\":4294967295,\"i\":4294967295}}", 4294967295L, 4294967295L));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            " ", "not-a-timestamp", "-1", "4294967296", "1789084800000", "9223372036854775808", "30.5", "3e1",
            "1969-12-31T23:59:59Z", "2106-02-07T06:28:16Z", "2026-09-11", "2026-09-11T00:00:00",
            "2026-09-11T00:00:00.001Z", "2026-09-11T00:00:00.000000001Z", "2026-02-29T00:00:00Z",
            "{}", "[]", "null", "{\"$date\":\"2026-09-11T00:00:00Z\"}", "Timestamp(30, 0)",
            "{\"$timestamp\":null}", "{\"$timestamp\":{\"t\":30}}", "{\"$timestamp\":{\"i\":0}}",
            "{\"$timestamp\":{\"other\":30,\"i\":0}}", "{\"$timestamp\":{\"t\":30,\"other\":0}}",
            "{\"$timestamp\":{\"t\":30,\"i\":0,\"extra\":1}}", "{\"$timestamp\":{\"t\":30,\"i\":0},\"extra\":1}",
            "{\"$timestamp\":{\"t\":-1,\"i\":0}}", "{\"$timestamp\":{\"t\":30,\"i\":-1}}",
            "{\"$timestamp\":{\"t\":4294967296,\"i\":0}}", "{\"$timestamp\":{\"t\":30,\"i\":4294967296}}",
            "{\"$timestamp\":{\"t\":9223372036854775808,\"i\":0}}", "{\"$timestamp\":{\"t\":30,\"i\":9223372036854775808}}",
            "{\"$timestamp\":{\"t\":30.5,\"i\":0}}", "{\"$timestamp\":{\"t\":30,\"i\":0.5}}",
            "{\"$timestamp\":{\"t\":\"30\",\"i\":0}}", "{\"$timestamp\":{\"t\":30,\"i\":null}}",
            "{\"$timestamp\":{\"t\":30,\"i\":0,\"t\":31}}",
            "{\"$timestamp\":{\"t\":30,\"i\":0},\"$timestamp\":{\"t\":31,\"i\":0}}",
            "{\"$timestamp\":{\"t\":30,\"i\":0}} {}", "{\"$timestamp\":{\"t\":30,\"i\":0}} trailing",
            "{\"$timestamp\":{\"t\":30,\"i\":0}"
    })
    void shouldRejectInvalidTimestamp(String value) {
        assertThatThrownBy(() -> BsonTimestampParser.parse(value)).isInstanceOf(IllegalArgumentException.class);
    }
}
