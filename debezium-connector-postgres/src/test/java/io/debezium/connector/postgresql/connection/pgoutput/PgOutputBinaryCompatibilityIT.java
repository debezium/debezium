/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection.pgoutput;

import java.sql.SQLException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.TestHelper;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.junit.SkipTestOnPostgresVersion;

/**
 * A collection of tests to ensure Postgres binary representation did not change between Postgres version. The test
 * uses Postgres {@code ..._send} functions to get the binary representation using SQL. These send functions are the same ones
 * used in `pgoutput` binary mode.
 *<p>
 * To get the send function of a root postgres type, you can run SQL like this:
 * <pre>{@code
 * SELECT t.typname, t.typsend::regproc AS send_function
 * FROM pg_type t
 * WHERE t.typname = 'int4';
 * }</pre>
 */
@SkipTestOnPostgresVersion(lessThan = 14)
public class PgOutputBinaryCompatibilityIT {

    @BeforeAll
    public static void beforeClass() throws SQLException {
        try (var connection = TestHelper.create()) {
            connection.execute("CREATE EXTENSION IF NOT EXISTS postgis;");
            connection.execute("CREATE TYPE bin_mode_mood AS ENUM ('sad', 'ok', 'happy');");
        }
    }

    @AfterAll
    public static void afterClass() throws SQLException {
        try (var connection = TestHelper.create()) {
            connection.execute("DROP TYPE bin_mode_mood;");
        }
    }

    @Test
    void shouldReceiveBinaryRepresenationInKnownFormat() throws SQLException {
        try (var connection = TestHelper.create()) {
            connection.connect();

            connection.execute("SET TIMEZONE TO 'UTC';");

            // One byte
            assertSendValue(connection, "select encode(boolsend(true), 'hex')", "01");
            assertSendValue(connection, "select encode(boolsend(false), 'hex')", "00");

            assertSendValue(connection, "select encode(charsend('B'), 'hex')", "42");

            // Integers
            assertSendValue(connection, "select encode(int2send(1984::int2), 'hex')", "07c0");
            assertSendValue(connection, "select encode(int4send(1984), 'hex')", "000007c0");
            assertSendValue(connection, "select encode(int8send(1984::bigint), 'hex')", "00000000000007c0");
            assertSendValue(connection, "select encode(oidsend(21::oid), 'hex');", "00000015");

            // Decimals
            assertSendValue(connection, "select encode(float4send(3.14159265359), 'hex')", "40490fdb");
            assertSendValue(connection, "select encode(float8send(3.14159265359), 'hex')", "400921fb54442eea");
            assertSendValue(connection, "select encode(numeric_send(3.14159265358979323846264338327950288419716939937510), 'hex')",
                    "000e0000000000320003058724310e051efc0f060a530ef81f0e0b4407b31b1b249f03e8");
            assertSendValue(connection, "select encode(numeric_send('NaN'::numeric), 'hex')",
                    "00000000c0000000");
            assertSendValue(connection, "select encode(numeric_send('infinity'::numeric), 'hex')",
                    "00000000d0000020");
            assertSendValue(connection, "select encode(numeric_send('-infinity'::numeric), 'hex')",
                    "00000000f0000020");
            assertSendValue(connection, "select encode(cash_send('19.99'::money), 'hex')", "00000000000007cf");

            // Textual
            assertSendValue(connection, "select encode(textsend('Four legs good, two legs bad.'), 'hex')",
                    "466f7572206c65677320676f6f642c2074776f206c656773206261642e");
            assertSendValue(connection, "select encode(varcharsend('Four legs good, two legs bad.'), 'hex')",
                    "466f7572206c65677320676f6f642c2074776f206c656773206261642e");
            assertSendValue(connection, "select encode(bpcharsend('Four legs good, two legs bad.'), 'hex')",
                    "466f7572206c65677320676f6f642c2074776f206c656773206261642e");
            assertSendValue(connection, "select encode(byteasend('Four legs good, two legs bad.'), 'hex')",
                    "466f7572206c65677320676f6f642c2074776f206c656773206261642e");

            // ISBN
            // ISBN does not have a binary send function. It's always sent in text mode.

            // Temporal
            assertSendValue(connection, "select encode(date_send('1-1-2000'), 'hex')", "00000000");
            assertSendValue(connection, "select encode(date_send('12-31-1999'), 'hex')", "ffffffff");
            assertSendValue(connection, "select encode(time_send('14:30:00'), 'hex')", "0000000c275cca00");
            assertSendValue(connection, "select encode(timetz_send(TIMETZ '14:30:00' AT TIME ZONE 'Africa/Cairo'), 'hex')",
                    "0000000eab17b600ffffd5d0");
            assertSendValue(connection, "select encode(timestamp_send('2026-10-19 14:30:00'), 'hex')",
                    "000301311e174a00");
            assertSendValue(connection, "select encode(timestamptz_send(TIMESTAMPTZ '2026-10-19 14:30:00' AT TIME ZONE 'Africa/Cairo'), 'hex')",
                    "00030133a1d23600");
            assertSendValue(connection, "select encode(interval_send('1 day 4 hours 30 minutes'::INTERVAL), 'hex')",
                    "00000003c59862000000000100000000");

            // Bits
            assertSendValue(connection, "select encode(bit_send('11111000000'), 'hex')", "0000000bf800");
            assertSendValue(connection, "select encode(bit_send('1000000000000000000001'), 'hex');", "00000016800004");
            assertSendValue(connection, "select encode(varbit_send('11111000000'), 'hex')", "0000000bf800");
            assertSendValue(connection, "select encode(varbit_send('1000000000000000000001'), 'hex');", "00000016800004");

            // Documents
            assertSendValue(connection, "select encode(json_send('{\"id\": 7, \"name\": \"George Orwell\"}'::json), 'hex');",
                    "7b226964223a20372c20226e616d65223a202247656f726765204f7277656c6c227d");
            assertSendValue(connection, "select encode(jsonb_send('{\"id\": 7, \"name\": \"George Orwell\"}'::jsonb), 'hex');",
                    // Unlike JSON, JSONB is prefixed with the version, hence the 01 value for the first byte
                    "017b226964223a20372c20226e616d65223a202247656f726765204f7277656c6c227d");
            assertSendValue(connection, "select encode(xml_send('<root><writer id=\"1984\">George Orwell</writer></root>'), 'hex');",
                    "3c726f6f743e3c7772697465722069643d2231393834223e47656f726765204f7277656c6c3c2f7772697465723e3c2f726f6f743e");

            // Misc
            assertSendValue(connection, "select encode(uuid_send('485f5bf1-ff4b-4a36-a4d1-c08594c6ef6c'::uuid), 'hex');",
                    "485f5bf1ff4b4a36a4d1c08594c6ef6c");
            assertSendValue(connection, "select encode(inet_send('208.80.152.2/16'), 'hex');", "02100004d0509802");
            assertSendValue(connection, "select encode(cidr_send('208.80.152.2'), 'hex');", "02200104d0509802");
            assertSendValue(connection, "select encode(macaddr_send('08:00:2b:01:02:03'), 'hex');", "08002b010203");
            assertSendValue(connection, "select encode(macaddr8_send('08-00-2b-ff-fe-01-02-03'), 'hex');",
                    "08002bfffe010203");

            // Geometric
            assertSendValue(connection, "select encode(point_send(point '(10.5, 20.0)'), 'hex');",
                    "40250000000000004034000000000000");
            assertSendValue(connection, "select encode(lseg_send(lseg '((10.5, 20.0), (30.0, 40.5))'), 'hex');",
                    "40250000000000004034000000000000403e0000000000004044400000000000");
            assertSendValue(connection, "select encode(path_send(path '[(10.0, 10.0), (20.0, 30.0), (40.0, 40.0)]'), 'hex');",
                    "0000000003402400000000000040240000000000004034000000000000403e00000000000040440000000000004044000000000000");
            assertSendValue(connection, "select encode(box_send(box '((10.0, 20.0), (30.0, 40.5))'), 'hex');",
                    "403e000000000000404440000000000040240000000000004034000000000000");
            assertSendValue(connection, "select encode(poly_send(polygon '((10.0, 10.0), (20.0, 30.0), (40.0, 10.0))'), 'hex');",
                    "00000003402400000000000040240000000000004034000000000000403e00000000000040440000000000004024000000000000");
            assertSendValue(connection, "select encode(line_send(line '((1.5, 2.0), (3.5, 4.0))'), 'hex');",
                    "3ff0000000000000bff00000000000003fe0000000000000");
            assertSendValue(connection, "select encode(circle_send(circle '((40.7128, -74.0060), 2.5)'), 'hex');",
                    "40445b3d07c84b5ec05280624dd2f1aa4004000000000000");

            // Arrays
            assertSendValue(connection, "select encode(array_send(ARRAY['Kafka', 'Postgres']), 'hex');",
                    "0000000100000000000000190000000200000001000000054b61666b6100000008506f737467726573");
            assertSendValue(connection, "select encode(array_send(ARRAY[1, 2, 3, 4]), 'hex');",
                    "00000001000000000000001700000004000000010000000400000001000000040000000200000004000000030000000400000004");
            assertSendValue(connection, "select encode(array_send(ARRAY[1.1, 1.2]), 'hex');",
                    "0000000100000000000006a400000002000000010000000c0002000000000001000103e80000000c0002000000000001000107d0");
            assertSendValue(connection, "select encode(array_send(ARRAY[true, false, true]), 'hex');",
                    "0000000100000000000000100000000300000001000000010100000001000000000101");

            // Ranges
            assertSendValue(connection, "select encode(range_send('(,)'::int4range), 'hex');", "18");
            assertSendValue(connection, "select encode(range_send('(1,100)'::int4range), 'hex');",
                    "0200000004000000020000000400000064");
            assertSendValue(connection, "select encode(range_send('(1,100]'::int4range), 'hex');",
                    "0200000004000000020000000400000065");
            assertSendValue(connection, "select encode(range_send('[1,100]'::int4range), 'hex');",
                    "0200000004000000010000000400000065");
            assertSendValue(connection, "select encode(range_send('[,100]'::int4range), 'hex');", "080000000400000065");
            assertSendValue(connection, "select encode(range_send('[1984-1-1, 1984-12-31]'::daterange), 'hex');",
                    "0200000004ffffe92c00000004ffffea9a");
            assertSendValue(connection, "select encode(range_send('[2.7182818284, 3.14159265359)'::numrange), 'hex');",
                    "0200000010000400000000000a00021c0e1ff620d000000010000400000000000b0003058724310e06");
            assertSendValue(connection, "select encode(range_send('[1969-07-16 13:32:00+00, 1969-07-24 16:50:35+00)'::tsrange), 'hex');",
                    "0200000008fffc95c26989f40000000008fffc96661e756cc0");

            // Enums
            assertSendValue(connection, "select encode(enum_send('happy'::bin_mode_mood), 'hex');", "6861707079");

            // PostGIS
            assertSendValue(connection, "select encode(geometry_send('POINT(12.9716 77.5946)'::geometry), 'hex');",
                    "01010000005396218e75f12940e78c28ed0d665340");
            assertSendValue(connection, "select encode(geography_send('POLYGON((-74.0 40.7, -73.9 40.7, -73.9 40.8, -74.0 40.8, -74.0 40.7))'::geography), 'hex');",
                    "0103000020e6100000010000000500000000000000008052c09a999999995944409a999999997952c09a999999995944409a999999997952c0666666666666444000000000008052c0666666666666444000000000008052c09a99999999594440");
        }
    }

    private void assertSendValue(final PostgresConnection connection, final String sql, final String value)
            throws SQLException {
        connection.query(sql, rs -> {
            rs.next();

            Assertions.assertThat(rs.getString(1))
                    .isEqualTo(value);
        });
    }
}
