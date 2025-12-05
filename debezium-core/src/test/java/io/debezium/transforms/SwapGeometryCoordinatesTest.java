/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geometry;
import io.debezium.doc.FixFor;
import io.debezium.util.HexConverter;

/**
 * Unit test for the {@link SwapGeometryCoordinates} transformation.
 *
 * @author Chris Cranford
 */
public class SwapGeometryCoordinatesTest {

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().optional()
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
            .build();

    private static final Schema RECORD_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT8_SCHEMA)
            .field("geo", Geometry.schema())
            .build();

    private final SwapGeometryCoordinates<SourceRecord> converter = new SwapGeometryCoordinates<>();

    @Test
    @FixFor("DBZ-9556")
    public void testSwapPointGeometry() {
        // In ESPG format
        final String wkbHex = "010100000000000000000020400000000000003340";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex, "010100000000000000000033400000000000002040");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapLineStringGeometryDebeziumEvent() {
        // In ESPG format
        final String wkbHex = "0102000000030000000000000000003e40000000000000244000000000000024400000000000003e" +
                "4000000000000044400000000000004440";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "01020000000300000000000000000024400000000000003e400000000000003e4000000000000024" +
                        "4000000000000044400000000000004440");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapPolygon() {
        // In ESPG format
        final String wkbHex = "010300000001000000050000000000000000003e4000000000000024400000000000004440000000" +
                "00000044400000000000003440000000000000444000000000000024400000000000003440000000" +
                "0000003e400000000000002440";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "0103000000010000000500000000000000000024400000000000003e400000000000004440000000" +
                        "00000044400000000000004440000000000000344000000000000034400000000000002440000000" +
                        "00000024400000000000003e40");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapPolygonWithMultipleRings() {
        // In ESPG format
        final String wkbHex = "01030000000200000005000000000000000080414000000000000024400000000000804640000000" +
                "00008046400000000000002e40000000000000444000000000000024400000000000003440000000" +
                "000080414000000000000024400400000000000000000034400000000000003e4000000000008041" +
                "4000000000008041400000000000003e40000000000000344000000000000034400000000000003e" +
                "40";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "01030000000200000005000000000000000000244000000000008041400000000000804640000000" +
                        "000080464000000000000044400000000000002e4000000000000034400000000000002440000000" +
                        "00000024400000000000804140040000000000000000003e40000000000000344000000000008041" +
                        "40000000000080414000000000000034400000000000003e400000000000003e4000000000000034" +
                        "40");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapMultiPoint() {
        // In ESPG format
        final String wkbHex = "01040000000400000001010000000000000000002440000000000000444001010000000000000000" +
                "0044400000000000003e400101000000000000000000344000000000000034400101000000000000" +
                "0000003e400000000000002440";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "01040000000400000001010000000000000000004440000000000000244001010000000000000000" +
                        "003e4000000000000044400101000000000000000000344000000000000034400101000000000000" +
                        "00000024400000000000003e40");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapMultiLineString() {
        // In ESPG format
        final String wkbHex = "01050000000200000001020000000300000000000000000024400000000000002440000000000000" +
                "34400000000000003440000000000000244000000000000044400102000000040000000000000000" +
                "00444000000000000044400000000000003e400000000000003e4000000000000044400000000000" +
                "0034400000000000003e400000000000002440";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "01050000000200000001020000000300000000000000000024400000000000002440000000000000" +
                        "34400000000000003440000000000000444000000000000024400102000000040000000000000000" +
                        "00444000000000000044400000000000003e400000000000003e4000000000000034400000000000" +
                        "00444000000000000024400000000000003e40");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapMultiPolygon() {
        // In ESPG format
        final String wkbHex = "01060000000200000001030000000100000004000000000000000000444000000000000044400000" +
                "000000003440000000000080464000000000008046400000000000003e4000000000000044400000" +
                "00000000444001030000000200000006000000000000000000344000000000008041400000000000" +
                "0024400000000000003e40000000000000244000000000000024400000000000003e400000000000" +
                "00144000000000008046400000000000003440000000000000344000000000008041400400000000" +
                "00000000003e40000000000000344000000000000034400000000000002e40000000000000344000" +
                "000000000039400000000000003e400000000000003440";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "01060000000200000001030000000100000004000000000000000000444000000000000044400000" +
                        "00000080464000000000000034400000000000003e40000000000080464000000000000044400000" +
                        "00000000444001030000000200000006000000000000000080414000000000000034400000000000" +
                        "003e4000000000000024400000000000002440000000000000244000000000000014400000000000" +
                        "003e4000000000000034400000000000804640000000000080414000000000000034400400000000" +
                        "000000000034400000000000003e400000000000002e400000000000003440000000000000394000" +
                        "0000000000344000000000000034400000000000003e40");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapGeometryCollection() {
        // In ESPG format
        final String wkbHex = "01070000000700000001010000000000000000002040000000000000334001020000000300000000" +
                "00000000003e40000000000000244000000000000024400000000000003e40000000000000444000" +
                "00000000004440010300000001000000050000000000000000003e40000000000000244000000000" +
                "00004440000000000000444000000000000034400000000000004440000000000000244000000000" +
                "000034400000000000003e4000000000000024400103000000020000000500000000000000008041" +
                "400000000000002440000000000080464000000000008046400000000000002e4000000000000044" +
                "40000000000000244000000000000034400000000000804140000000000000244004000000000000" +
                "00000034400000000000003e40000000000080414000000000008041400000000000003e40000000" +
                "000000344000000000000034400000000000003e4001040000000400000001010000000000000000" +
                "0024400000000000004440010100000000000000000044400000000000003e400101000000000000" +
                "0000003440000000000000344001010000000000000000003e400000000000002440010500000002" +
                "00000001020000000300000000000000000024400000000000002440000000000000344000000000" +
                "00003440000000000000244000000000000044400102000000040000000000000000004440000000" +
                "00000044400000000000003e400000000000003e4000000000000044400000000000003440000000" +
                "0000003e400000000000002440010600000002000000010300000001000000040000000000000000" +
                "00444000000000000044400000000000003440000000000080464000000000008046400000000000" +
                "003e4000000000000044400000000000004440010300000002000000060000000000000000003440" +
                "000000000080414000000000000024400000000000003e4000000000000024400000000000002440" +
                "0000000000003e400000000000001440000000000080464000000000000034400000000000003440" +
                "0000000000804140040000000000000000003e400000000000003440000000000000344000000000" +
                "00002e40000000000000344000000000000039400000000000003e400000000000003440";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex,
                "01070000000700000001010000000000000000003340000000000000204001020000000300000000" +
                        "000000000024400000000000003e400000000000003e400000000000002440000000000000444000" +
                        "000000000044400103000000010000000500000000000000000024400000000000003e4000000000" +
                        "00004440000000000000444000000000000044400000000000003440000000000000344000000000" +
                        "0000244000000000000024400000000000003e400103000000020000000500000000000000000024" +
                        "4000000000008041400000000000804640000000000080464000000000000044400000000000002e" +
                        "40000000000000344000000000000024400000000000002440000000000080414004000000000000" +
                        "0000003e400000000000003440000000000080414000000000008041400000000000003440000000" +
                        "0000003e400000000000003e40000000000000344001040000000400000001010000000000000000" +
                        "004440000000000000244001010000000000000000003e4000000000000044400101000000000000" +
                        "00000034400000000000003440010100000000000000000024400000000000003e40010500000002" +
                        "00000001020000000300000000000000000024400000000000002440000000000000344000000000" +
                        "00003440000000000000444000000000000024400102000000040000000000000000004440000000" +
                        "00000044400000000000003e400000000000003e4000000000000034400000000000004440000000" +
                        "00000024400000000000003e40010600000002000000010300000001000000040000000000000000" +
                        "0044400000000000004440000000000080464000000000000034400000000000003e400000000000" +
                        "80464000000000000044400000000000004440010300000002000000060000000000000000804140" +
                        "00000000000034400000000000003e40000000000000244000000000000024400000000000002440" +
                        "00000000000014400000000000003e40000000000000344000000000008046400000000000804140" +
                        "00000000000034400400000000000000000034400000000000003e400000000000002e4000000000" +
                        "000034400000000000003940000000000000344000000000000034400000000000003e40");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    @Test
    @FixFor("DBZ-9556")
    public void testSwapEmptyGeometryCollection() {
        // In ESPG format
        final String wkbHex = "010700000000000000";

        // SRID 4326 should be swapped
        // Converts ESPG format to traditional GIS format
        assertSwapped(4326, wkbHex, "010700000000000000");

        // SRD 3557 should not be swapped
        assertNotSwapped(3557, wkbHex);
    }

    private void assertSwapped(Integer srid, String wkb, String expectedWkb) {
        applyTransformEnvelopePayload(srid, wkb, expectedWkb);
        applyTransformFlattenedPayload(srid, wkb, expectedWkb);
    }

    private void assertNotSwapped(Integer srid, String wkb) {
        // Checks that the transformation is a no-op for the given SRID
        applyTransformEnvelopePayload(srid, wkb, wkb);
        applyTransformFlattenedPayload(srid, wkb, wkb);
    }

    private void applyTransformFlattenedPayload(Integer srid, String wkb, String expectedWkb) {
        final Map<String, String> properties = new HashMap<>();
        converter.configure(properties);

        final Schema geometrySchema = Geometry.schema();
        final byte[] wkbBytes = HexConverter.convertFromHex(wkb);

        final Struct payload = new Struct(RECORD_SCHEMA);
        payload.put("id", (byte) 1);
        payload.put("geo", Geometry.createValue(geometrySchema, wkbBytes, srid));

        final SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                payload.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);
        final Struct transformedValue = requireStruct(transformedRecord.value(), "value should be a struct");

        final byte[] newWkb = HexConverter.convertFromHex(expectedWkb);

        final Struct geo = transformedValue.getStruct("geo");
        assertThat(HexConverter.convertToHexString(geo.getBytes(Geometry.WKB_FIELD))).isEqualTo(expectedWkb);
        assertThat(geo.get(Geometry.SRID_FIELD)).isEqualTo(srid);

        assertThat(geo).isEqualTo(Geometry.createValue(geometrySchema, newWkb, srid));
    }

    private void applyTransformEnvelopePayload(Integer srid, String wkb, String expectedWkb) {
        final Map<String, String> properties = new HashMap<>();
        converter.configure(properties);

        final Schema geometrySchema = Geometry.schema();
        final byte[] wkbBytes = HexConverter.convertFromHex(wkb);

        final Struct after = new Struct(RECORD_SCHEMA);
        after.put("id", (byte) 1);
        after.put("geo", Geometry.createValue(geometrySchema, wkbBytes, srid));

        final Struct source = new Struct(SOURCE_SCHEMA);
        source.put("table", "geos");
        source.put("lsn", 1);
        source.put("ts_ms", 123456789);

        final Envelope envelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(RECORD_SCHEMA)
                .withSource(SOURCE_SCHEMA)
                .build();

        final Struct payload = envelope.create(after, source, Instant.now());

        final SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);

        final SourceRecord transformedRecord = converter.apply(record);

        final Struct transformedValue = requireStruct(transformedRecord.value(), "value should be a struct");
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        assertThat(transformedAfter.schema().field("geo").schema()).isEqualTo(geometrySchema);

        final byte[] newWkb = HexConverter.convertFromHex(expectedWkb);

        final Struct geo = transformedAfter.getStruct("geo");
        assertThat(HexConverter.convertToHexString(geo.getBytes(Geometry.WKB_FIELD))).isEqualTo(expectedWkb);
        assertThat(geo.get(Geometry.SRID_FIELD)).isEqualTo(srid);

        assertThat(geo).isEqualTo(Geometry.createValue(geometrySchema, newWkb, srid));
    }
}
