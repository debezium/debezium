/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.data.Envelope;
import io.debezium.data.geometry.Geometry;
import io.debezium.util.HexConverter;

/**
 * Unit test for the {@link GeometryFormatTransformer} transformation for Point, LineString, Polygon and MultiPoint geometries.
 *
 */
public class GeometryFormatTransformerTest {

    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct().optional()
            .field("table", Schema.STRING_SCHEMA)
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
            .build();
    private static final Schema RECORD_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT8_SCHEMA)
            .field("geo", Geometry.schema())
            .build();
    private static final String POINT_GEO_WKB_HEX = "010100000000000000000020400000000000003340";
    private static final String POINT_GEO_EWKB_HEX = "0101000020e610000000000000000020400000000000003340";
    private static final String LINE_STRING_GEO_WKB_HEX = "0102000000030000000000000000003e40000000000000244000000000000024400000000000003e4000000000000044400000000000004440";
    private static final String LINE_STRING_GEO_EWKB_HEX = "0102000020e6100000030000000000000000003e40000000000000244000000000000024400000000000003e4000000000000044400000000000004440";
    private static final String POLYGON_GEO_WKB_HEX = "010300000001000000050000000000000000003e4000000000000024400000000000004440000000" +
            "000000444000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440";
    private static final String POLYGON_GEO_EWKB_HEX = "0103000020e610000001000000050000000000000000003e400000000000002440000000000000444000000000000044400000000000003440000000000000"
            +
            "4440000000000000244000000000000034400000000000003e400000000000002440";
    private static final String POLYGON_MULTIPLE_RINGS_GEO_WKB_HEX = "01030000000200000005000000000000000080414000000000000024400000000000804640000000" +
            "00008046400000000000002e40000000000000444000000000000024400000000000003440000000000080414000000000000024400400000000000000000034400000000000003e4000000000008041"
            +
            "4000000000008041400000000000003e40000000000000344000000000000034400000000000003e40";
    private static final String POLYGON_MULTIPLE_RINGS_GEO_EWKB_HEX = "0103000020e6100000020000000500000000000000008041400000000000002440000000000080464000000000008046400000000000002e4"
            +
            "0000000000000444000000000000024400000000000003440000000000080414000000000000024400400000000000000000034400000000000003e40000000000080414000000000008041400000000000003e40000000000000344000000000000034400000000000003e40";
    private static final String MULTI_POINT_GEO_WKB_HEX = "01040000000400000001010000000000000000002440000000000000444001010000000000000000" +
            "0044400000000000003e4001010000000000000000003440000000000000344001010000000000000000003e400000000000002440";
    private static final String MULTI_POINT_GEO_EWKB_HEX = "0104000020e610000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003e400101000000000000000000"
            +
            "3440000000000000344001010000000000000000003e400000000000002440";
    private static final String MULTI_LINE_STRING_GEO_WKB_HEX = "0105000000020000000102000000030000000000000000002440000000000000244000000000000034400000000000003440000000000000244000000000000044400102000000040000000000000000"
            +
            "00444000000000000044400000000000003e400000000000003e40000000000000444000000000000034400000000000003e400000000000002440";
    private static final String MULTI_LINE_STRING_GEO_EWKB_HEX = "0105000020e610000002000000010200000003000000000000000000244000000000000024400000000000003440000000000000344000000000000024400000000000004440010200000004000000000000000000"
            +
            "444000000000000044400000000000003e400000000000003e40000000000000444000000000000034400000000000003e400000000000002440";
    private static final String GEOMETRY_COLLECTION_WKB_HEX = "0107000000070000000101000000000000000000204000000000000033400102000000030000000000000000003e40000000000000244000000000000024400000000000003e40000000000000444000"
            +
            "00000000004440010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444000000000000034400000000000004440000000000000244000000000000034400000000000003e4000000000000024400103000000020000000500000000000000008041"
            +
            "400000000000002440000000000080464000000000008046400000000000002e40000000000000444000000000000024400000000000003440000000000080414000000000000024400400000000000000000034400000000000003e40000000000080414000000000008041400000000000003e400000000"
            +
            "00000344000000000000034400000000000003e40010400000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003e4001010000000000000000003440000000000000344001010000000000000000003e4000000000000024400105000000020000"
            +
            "00010200000003000000000000000000244000000000000024400000000000003440000000000000344000000000000024400000000000004440010200000004000000000000000000444000000000000044400000000000003e400000000000003e40000000000000444000000000000034400000000000003e4"
            +
            "0000000000000244001060000000200000001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003e400000000000004440000000000000444001030000000200000006000000000000000000344000000000008041400"
            +
            "0000000000024400000000000003e40000000000000244000000000000024400000000000003e4000000000000014400000000000804640000000000000344000000000000034400000000000804140040000000000000000003e40000000000000344000000000000034400000000000002e40000000000000344000000000000039400000000000003e400000000000003440";
    private static final String GEOMETRY_COLLECTION_EWKB_HEX = "0107000020e6100000070000000101000000000000000000204000000000000033400102000000030000000000000000003e40000000000000244000000000000024400000000000003e4000000000000044400000000000004440010300000001"
            +
            "000000050000000000000000003e4000000000000024400000000000004440000000000000444000000000000034400000000000004440000000000000244000000000000034400000000000003e4000000000000024400103000000020000000500000000000000008041400000000000002440000000000080464"
            +
            "000000000008046400000000000002e40000000000000444000000000000024400000000000003440000000000080414000000000000024400400000000000000000034400000000000003e40000000000080414000000000008041400000000000003e40000000000000344000000000000034400000000000003e4"
            +
            "0010400000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003e4001010000000000000000003440000000000000344001010000000000000000003e4000000000000024400105000000020000000102000000030000000000000000002440000000000000"
            +
            "24400000000000003440000000000000344000000000000024400000000000004440010200000004000000000000000000444000000000000044400000000000003e400000000000003e40000000000000444000000000000034400000000000003e40000000000000244001060000000200000001030000000100000004"
            +
            "000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003e4000000000000044400000000000004440010300000002000000060000000000000000003440000000000080414000000000000024400000000000003e400000000000002440000000000000244"
            +
            "00000000000003e4000000000000014400000000000804640000000000000344000000000000034400000000000804140040000000000000000003e40000000000000344000000000000034400000000000002e40000000000000344000000000000039400000000000003e400000000000003440";
    private static final String MULTI_POLYGON_GEO_WKB_HEX = "01060000000200000001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003e4000000000000044400000"
            +
            "000000004440010300000002000000060000000000000000003440000000000080414000000000000024400000000000003e40000000000000244000000000000024400000000000003e4000000000000014400000000000804640000000000000344000000000000034400000000000804140040000000000000000003e4"
            +
            "0000000000000344000000000000034400000000000002e40000000000000344000000000000039400000000000003e400000000000003440";
    private static final String MULTI_POLYGON_GEO_EWKB_HEX = "0106000020e61000000200000001030000000100000004000000000000000000444000000000000044400000000000003440000000000080464000000000008046400000000000003e4000000000000044400000000000004440010300000002000000060000000"
            +
            "000000000003440000000000080414000000000000024400000000000003e40000000000000244000000000000024400000000000003e4000000000000014400000000000804640000000000000344000000000000034400000000000804140040000000000000000003e4000000000000034400000000000003440000000000000"
            +
            "2e40000000000000344000000000000039400000000000003e400000000000003440";
    private static final String EMPTY_GEOMETRY_WKB_HEX = "010700000000000000";
    private static final String EMPTY_GEOMETRY_EWKB_HEX = "0107000020e610000000000000";

    private final GeometryFormatTransformer<SourceRecord> geometryFormatTransformer = new GeometryFormatTransformer<>();

    /**
     * Test transforming Point geometry from WKB to EWKB.
     */
    @Test
    public void testTransformPointGeometryWkbToEwkb() {

        assertFormatTransformation(4326, POINT_GEO_WKB_HEX, POINT_GEO_EWKB_HEX);

        assertFormatTransformationException(null, POINT_GEO_WKB_HEX);

    }

    /**
     * Test transforming Point geometry from EWKB to WKB.
     */
    @Test
    public void testTransformPointGeometryEwkbToWkb() {

        assertFormatTransformation(4326, POINT_GEO_EWKB_HEX, POINT_GEO_WKB_HEX);

    }

    /**
     * Test transforming LineString geometry from WKB to EWKB.
     */
    @Test
    public void testTransformLineStringWkbToEwkb() {

        assertFormatTransformation(4326, LINE_STRING_GEO_WKB_HEX, LINE_STRING_GEO_EWKB_HEX);

        assertFormatTransformationException(null, LINE_STRING_GEO_WKB_HEX);
    }

    /**
     * Test transforming LineString geometry from EWKB to WKB.
     */
    @Test
    public void testTransformLineStringEWkbToWkb() {

        assertFormatTransformation(4326, LINE_STRING_GEO_EWKB_HEX, LINE_STRING_GEO_WKB_HEX);

    }

    /**
     * Test transforming Polygon geometry from WKB to EWKB.
     */
    @Test
    public void testTransformPolygonWkbToEwkb() {

        assertFormatTransformation(4326, POLYGON_GEO_WKB_HEX, POLYGON_GEO_EWKB_HEX);

        assertFormatTransformationException(null, POLYGON_GEO_WKB_HEX);
    }

    /**
     * Test transforming Polygon geometry from EWKB to WKB.
     */
    @Test
    public void testTransformPolygonEWkbToWkb() {

        assertFormatTransformation(4326, POLYGON_GEO_EWKB_HEX, POLYGON_GEO_WKB_HEX);

    }

    /**
     * Test transforming MultiPolygon geometry from WKB to EWKB.
     */
    @Test
    public void testTransformMultiPolygonWkbToEwkb() {

        assertFormatTransformation(4326, MULTI_POLYGON_GEO_WKB_HEX, MULTI_POLYGON_GEO_EWKB_HEX);

        assertFormatTransformationException(null, MULTI_POLYGON_GEO_WKB_HEX);
    }

    /**
     * Test transforming MultiPolygon geometry from EWKB to WKB.
     */
    @Test
    public void testTransformMultiPolygonEWkbToWkb() {

        assertFormatTransformation(4326, MULTI_POLYGON_GEO_EWKB_HEX, MULTI_POLYGON_GEO_WKB_HEX);

    }

    /**
     * Test transforming Polygon with multiple rings geometry from WKB to EWKB.
     */
    @Test
    public void testTransformPolygonWithMultipleRingsWkbToEwkb() {

        assertFormatTransformation(4326, POLYGON_MULTIPLE_RINGS_GEO_WKB_HEX, POLYGON_MULTIPLE_RINGS_GEO_EWKB_HEX);

        assertFormatTransformationException(null, POLYGON_MULTIPLE_RINGS_GEO_WKB_HEX);
    }

    /**
     * Test transforming Polygon with multiple rings geometry from EWKB to WKB.
     */
    @Test
    public void testTransformPolygonWithMultipleRingsEWkbToWkb() {

        assertFormatTransformation(4326, POLYGON_MULTIPLE_RINGS_GEO_EWKB_HEX, POLYGON_MULTIPLE_RINGS_GEO_WKB_HEX);

    }

    /**
     * Test transforming Multi Line String geometry from WKB to EWKB.
     */
    @Test
    public void testTransformMultiLineStringWkbToEwkb() {

        assertFormatTransformation(4326, MULTI_LINE_STRING_GEO_WKB_HEX, MULTI_LINE_STRING_GEO_EWKB_HEX);

        assertFormatTransformationException(null, MULTI_LINE_STRING_GEO_WKB_HEX);
    }

    /**
     * Test transforming Multi Line String geometry from EWKB to WKB.
     */
    @Test
    public void testTransformMultiLineStringEWkbToWkb() {

        assertFormatTransformation(4326, MULTI_LINE_STRING_GEO_EWKB_HEX, MULTI_LINE_STRING_GEO_WKB_HEX);

    }

    /**
     * Test transforming geometry collection from WKB to EWKB.
     */
    @Test
    public void testTransformGeometryCollectionWkbToEwkb() {

        assertFormatTransformation(4326, GEOMETRY_COLLECTION_WKB_HEX, GEOMETRY_COLLECTION_EWKB_HEX);

        assertFormatTransformationException(null, GEOMETRY_COLLECTION_WKB_HEX);
    }

    /**
     * Test transforming geometry collection from EWKB to WKB.
     */
    @Test
    public void testTransformGeometryCollectionEWkbToWkb() {

        assertFormatTransformation(4326, GEOMETRY_COLLECTION_EWKB_HEX, GEOMETRY_COLLECTION_WKB_HEX);

    }

    /**
     * Test transforming MultiPoint geometry from WKB to EWKB.
     */
    @Test
    public void testTransformMultiPointWkbToEwkb() {

        assertFormatTransformation(4326, MULTI_POINT_GEO_WKB_HEX, MULTI_POINT_GEO_EWKB_HEX);

        assertFormatTransformationException(null, MULTI_POINT_GEO_WKB_HEX);
    }

    /**
     * Test transforming MultiPoint geometry from EWKB to WKB.
     */
    @Test
    public void testTransformMultiPointEWkbToWkb() {

        assertFormatTransformation(4326, MULTI_POINT_GEO_EWKB_HEX, MULTI_POINT_GEO_WKB_HEX);

    }

    /**
     * Test transforming Empty geometry from WKB to EWKB.
     */
    @Test
    public void testTransformEmptyGeometryWkbToEwkb() {

        assertFormatTransformation(4326, EMPTY_GEOMETRY_WKB_HEX, EMPTY_GEOMETRY_EWKB_HEX);

        assertFormatTransformationException(null, EMPTY_GEOMETRY_WKB_HEX);
    }

    /**
     * Test transforming Empty geometry from EWKB to WKB.
     */
    @Test
    public void testTransformEmptyGeometryEWkbToWkb() {

        assertFormatTransformation(4326, EMPTY_GEOMETRY_EWKB_HEX, EMPTY_GEOMETRY_WKB_HEX);

    }

    /**
     * Helper method to assert that the geometry WKB is converted to EWKB and vice versa for envelop payloads and flattened payloads.
     * @param srid spacial reference id
     * @param inputFormatValue  can be WKB or EWKB
     * @param outputFormatValue  can be WKB or EWKB
     */
    private void assertFormatTransformation(Integer srid, String inputFormatValue, String outputFormatValue) {
        applyTransformEnvelopePayload(srid, inputFormatValue, outputFormatValue);
        applyTransformFlattenedPayload(srid, inputFormatValue, outputFormatValue);

    }

    /**
     * Helper method to assert that the geometry WKB to EWKB transformation throws an exception for unsupported SRIDs.
     * @param srid spatial reference id
     * @param wkb  well-known-binary
     */
    private void assertFormatTransformationException(Integer srid, String wkb) {
        applyTransformEnvelopePayloadException(srid, wkb);
        applyTransformFlattenedPayloadException(srid, wkb);
    }

    /**
     * Helper method to create a SourceRecord with flattened payload.
     * @param srid spatial reference id
     * @param inputFormatValue can be WKB or EWKB
     * @return SourceRecord
     */
    private SourceRecord getFlattenedPayload(Integer srid, String inputFormatValue) {
        final Map<String, String> properties = new HashMap<>();
        geometryFormatTransformer.configure(properties);

        final byte[] wkbBytes = HexConverter.convertFromHex(inputFormatValue);
        final Struct payload = new Struct(RECORD_SCHEMA);
        payload.put("id", (byte) 1);
        payload.put("geo", Geometry.createValue(Geometry.schema(), wkbBytes, srid));

        final SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                payload.schema(),
                payload);
        return record;
    }

    /**
     * Helper method to create a SourceRecord with envelope payload.
     * @param srid spatial reference id
     * @param wkb well-known-binary
     * @return SourceRecord
     */
    private SourceRecord getEnvelopPayload(Integer srid, String wkb) {
        final Map<String, String> properties = new HashMap<>();
        geometryFormatTransformer.configure(properties);

        final byte[] wkbBytes = HexConverter.convertFromHex(wkb);

        final Struct after = new Struct(RECORD_SCHEMA);
        after.put("id", (byte) 1);
        after.put("geo", Geometry.createValue(Geometry.schema(), wkbBytes, srid));

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

        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "db.server1.table1",
                envelope.schema(),
                payload);
    }

    /**
     * Applies the geometry format transformation to a SourceRecord with flattened payload and asserts the result.
     * @param srid spacial reference id
     * @param inputFormatValue can be WKB or EWKB
     * @param expectedOutputFormatValue can be WKB or EWKB
     */
    private void applyTransformFlattenedPayload(Integer srid, String inputFormatValue, String expectedOutputFormatValue) {

        final SourceRecord record = getFlattenedPayload(srid, inputFormatValue);

        final SourceRecord transformedRecord = geometryFormatTransformer.apply(record);

        final Struct transformedValue = requireStruct(transformedRecord.value(), "value should be a struct");

        final byte[] outputHexValue = HexConverter.convertFromHex(expectedOutputFormatValue);

        final Struct geo = transformedValue.getStruct("geo");
        assertThat(HexConverter.convertToHexString(geo.getBytes(Geometry.WKB_FIELD))).isEqualTo(expectedOutputFormatValue);
        assertThat(geo.get(Geometry.SRID_FIELD)).isEqualTo(srid);

        assertThat(geo).isEqualTo(Geometry.createValue(Geometry.schema(), outputHexValue, srid));

    }

    /**
     * Applies the geometry format transformation to a SourceRecord with envelope payload and asserts the result.
     * @param srid spacial reference id
     * @param inputFormatValue can be WKB or EWKB
     * @param outputFormatValue can be WKB or EWKB
     */
    private void applyTransformEnvelopePayload(Integer srid, String inputFormatValue, String outputFormatValue) {

        SourceRecord record = getEnvelopPayload(srid, inputFormatValue);

        final SourceRecord transformedRecord = geometryFormatTransformer.apply(record);

        final Struct transformedValue = requireStruct(transformedRecord.value(), "value should be a struct");
        final Struct transformedAfter = transformedValue.getStruct(Envelope.FieldName.AFTER);

        final byte[] newEkb = HexConverter.convertFromHex(outputFormatValue);

        final Struct geo = transformedAfter.getStruct("geo");
        assertThat(HexConverter.convertToHexString(geo.getBytes(Geometry.WKB_FIELD))).isEqualTo(outputFormatValue);
        assertThat(geo.get(Geometry.SRID_FIELD)).isEqualTo(srid);

        assertThat(geo).isEqualTo(Geometry.createValue(Geometry.schema(), newEkb, srid));
    }

    /**
     * Applies the geometry format transformation to a SourceRecord with flattened payload and asserts that an exception is thrown.
     * @param srid spacial reference id
     * @param wkb well-known-binary
     */
    private void applyTransformFlattenedPayloadException(Integer srid, String wkb) {
        SourceRecord record = getFlattenedPayload(srid, wkb);

        assetException(srid, record);

    }

    /**
     * Applies the geometry format transformation to a SourceRecord with envelope payload and asserts that an exception is thrown.
     * @param srid spacial reference id
     * @param wkb well-known-binary
     */
    private void applyTransformEnvelopePayloadException(Integer srid, String wkb) {
        SourceRecord record = getEnvelopPayload(srid, wkb);

        assetException(srid, record);

    }

    /**
     * Asserts that applying the geometry format transformation to the given SourceRecord throws an exception.
     * @param srid spacial reference id
     * @param record the SourceRecord to transform
     */
    private void assetException(Integer srid, SourceRecord record) {
        assertThatThrownBy(() -> geometryFormatTransformer.apply(record))
                .isInstanceOf(ConnectException.class)
                .hasMessage("Cannot convert to EWKB when SRID is null");
    }
}
