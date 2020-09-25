/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.DataType;

/**
 * This class ONLY tests the {@link CassandraTypeDeserializer#deserialize(AbstractType, ByteBuffer)}
 * method because the {@link CassandraTypeDeserializer#deserialize(DataType, ByteBuffer)} calls
 * converts the DataType (which is already tested in {@link CassandraTypeConverterTest} class) and
 * then delegates to the AbstractType method.
 */
public class CassandraTypeDeserializerTest {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    @Test
    public void testAscii() {
        String expectedAscii = "some text";

        ByteBuffer serializedAscii = AsciiType.instance.decompose(expectedAscii);

        Object deserializedAscii = CassandraTypeDeserializer.deserialize(AsciiType.instance, serializedAscii);

        Assert.assertEquals("some text", deserializedAscii);
    }

    @Test
    public void testBoolean() {
        Boolean expectedBoolean = true;

        ByteBuffer serializedBoolean = BooleanType.instance.decompose(expectedBoolean);

        Object deserializedBoolean = CassandraTypeDeserializer.deserialize(BooleanType.instance, serializedBoolean);

        Assert.assertEquals(expectedBoolean, deserializedBoolean);
    }

    @Test
    public void testBytes() {
        ByteBuffer expectedBytes = ByteBuffer.wrap("some random stuff here".getBytes(CHARSET));

        // Pretty sure this is a no-op, but for consistency...
        ByteBuffer serializedBytes = BytesType.instance.decompose(expectedBytes);

        Object deserializedBytes = CassandraTypeDeserializer.deserialize(BytesType.instance, serializedBytes);

        Assert.assertEquals(expectedBytes, deserializedBytes);
    }

    @Test
    public void testByteType() {
        Byte expectedByte = Byte.valueOf("9");

        ByteBuffer serializedByte = ByteType.instance.decompose(expectedByte);

        Object deserializedByte = CassandraTypeDeserializer.deserialize(ByteType.instance, serializedByte);

        Assert.assertEquals(expectedByte, deserializedByte);
    }

    @Test
    public void testCounterColumnType() {
        Long expectedCounterColumnType = 42L;

        ByteBuffer serializedCounter = CounterColumnType.instance.decompose(42L);

        Object deserializedCounter = CassandraTypeDeserializer.deserialize(CounterColumnType.instance, serializedCounter);

        Assert.assertEquals(expectedCounterColumnType, deserializedCounter);
    }

    @Test
    public void testDecimalType() {
        BigDecimal expectedDecimal = BigDecimal.valueOf(Math.PI);

        ByteBuffer serializedDecimal = DecimalType.instance.decompose(expectedDecimal);

        Object deserializedDecimal = CassandraTypeDeserializer.deserialize(DecimalType.instance, serializedDecimal);

        Assert.assertEquals(expectedDecimal, deserializedDecimal);
    }

    @Test
    public void testDoubleType() {
        Double expectedDouble = 100.5;

        ByteBuffer serializedDouble = DoubleType.instance.decompose(expectedDouble);

        Object deserializedDouble = CassandraTypeDeserializer.deserialize(DoubleType.instance, serializedDouble);

        Assert.assertEquals(expectedDouble, deserializedDouble);
    }

    @Test
    public void testDurationType() {
        Duration sourceDuration = Duration.newInstance(1, 3, 500);

        long expectedNanoDuration = (30 + 3) * ChronoUnit.DAYS.getDuration().toNanos() + 500;

        ByteBuffer serializedDuration = DurationType.instance.decompose(sourceDuration);

        Object deserializedDuration = CassandraTypeDeserializer.deserialize(DurationType.instance, serializedDuration);

        Assert.assertEquals(expectedNanoDuration, deserializedDuration);
    }

    @Test
    public void testFloatType() {
        Float expectedFloat = 66.6F;

        ByteBuffer serializedFloat = FloatType.instance.decompose(expectedFloat);

        Object deserializedFloat = CassandraTypeDeserializer.deserialize(FloatType.instance, serializedFloat);

        Assert.assertEquals(expectedFloat, deserializedFloat);
    }

    @Test
    public void testInetAddressType() throws UnknownHostException {
        InetAddress sourceInetAddress = InetAddress.getLocalHost();
        // the address is the only thing that cassandra will seralize for an inetadress.
        String expectedInetAddress = "/" + sourceInetAddress.getHostAddress();

        ByteBuffer serializedInetAddress = InetAddressType.instance.decompose(sourceInetAddress);

        Object deserializedInetAddress = CassandraTypeDeserializer.deserialize(InetAddressType.instance, serializedInetAddress);

        Assert.assertEquals(expectedInetAddress, deserializedInetAddress);
    }

    @Test
    public void testInt32Type() {
        Integer expectedInteger = 8;

        ByteBuffer serializedInt32 = Int32Type.instance.decompose(expectedInteger);

        Object deserializedInt32 = CassandraTypeDeserializer.deserialize(Int32Type.instance, serializedInt32);

        Assert.assertEquals(expectedInteger, deserializedInt32);
    }

    @Test
    public void testListType() {
        List<Integer> expectedList = new ArrayList<>();
        expectedList.add(1);
        expectedList.add(3);
        expectedList.add(5);

        // non-frozen
        ListType<Integer> nonFrozenListType = ListType.getInstance(Int32Type.instance, true);
        ByteBuffer serializedList = nonFrozenListType.decompose(expectedList);
        Object deserializedList = CassandraTypeDeserializer.deserialize(nonFrozenListType, serializedList);
        Assert.assertEquals(expectedList, deserializedList);

        // frozen
        ListType<Integer> frozenListType = ListType.getInstance(Int32Type.instance, false);
        serializedList = frozenListType.decompose(expectedList);
        deserializedList = CassandraTypeDeserializer.deserialize(frozenListType, serializedList);
        Assert.assertEquals(expectedList, deserializedList);
    }

    @Test
    public void testLongType() {
        Long expectedLong = 8L;

        ByteBuffer serializedLong = LongType.instance.decompose(expectedLong);

        Object deserializedLong = CassandraTypeDeserializer.deserialize(LongType.instance, serializedLong);

        Assert.assertEquals(expectedLong, deserializedLong);
    }

    @Test
    public void testMapType() {
        Map<String, Double> expectedMap = new HashMap<>();
        expectedMap.put("foo", 1D);
        expectedMap.put("bar", 50D);

        // non-frozen
        MapType<String, Double> nonFrozenMapType = MapType.getInstance(AsciiType.instance, DoubleType.instance, true);
        ByteBuffer serializedMap = nonFrozenMapType.decompose(expectedMap);
        Object deserializedMap = CassandraTypeDeserializer.deserialize(nonFrozenMapType, serializedMap);
        Assert.assertEquals(expectedMap, deserializedMap);

        // frozen
        MapType<String, Double> frozenMapType = MapType.getInstance(AsciiType.instance, DoubleType.instance, false);
        serializedMap = frozenMapType.decompose(expectedMap);
        deserializedMap = CassandraTypeDeserializer.deserialize(frozenMapType, serializedMap);
        Assert.assertEquals(expectedMap, deserializedMap);
    }

    @Test
    public void testMapTypeNonStringKeys() {
        Map<Integer, Float> sourceMap = new HashMap<>();
        sourceMap.put(1, 1.5F);
        sourceMap.put(2, 3.1414F);

        MapType<Integer, Float> mapType = MapType.getInstance(Int32Type.instance, FloatType.instance, true);
        ByteBuffer serializedMap = mapType.decompose(sourceMap);
        Object deserializedMap = CassandraTypeDeserializer.deserialize(mapType, serializedMap);

        Map<Integer, Float> expectedMap = new HashMap<>();
        expectedMap.put(1, 1.5F);
        expectedMap.put(2, 3.1414F);

        Assert.assertEquals(expectedMap, deserializedMap);
    }

    @Test
    public void testSetType() {
        Set<Float> sourceSet = new HashSet<>();
        sourceSet.add(42F);
        sourceSet.add(123F);

        // non-frozen
        SetType<Float> nonFrozenSetType = SetType.getInstance(FloatType.instance, true);
        ByteBuffer serializedSet = nonFrozenSetType.decompose(sourceSet);
        Collection<?> deserializedSet = (Collection<?>) CassandraTypeDeserializer.deserialize(nonFrozenSetType, serializedSet);
        // order may be different in the resulting collection.
        Assert.assertTrue(sourceSet.containsAll(deserializedSet));
        Assert.assertTrue(deserializedSet.containsAll(sourceSet));

        // frozen
        SetType<Float> frozenSetType = SetType.getInstance(FloatType.instance, false);
        serializedSet = frozenSetType.decompose(sourceSet);
        deserializedSet = (Collection<?>) CassandraTypeDeserializer.deserialize(frozenSetType, serializedSet);
        Assert.assertTrue(sourceSet.containsAll(deserializedSet));
        Assert.assertTrue(deserializedSet.containsAll(sourceSet));
    }

    @Test
    public void testShortType() {
        Short expectedShort = (short) 2;

        ByteBuffer serializedShort = ShortType.instance.decompose(expectedShort);

        Object deserializedShort = CassandraTypeDeserializer.deserialize(ShortType.instance, serializedShort);

        Assert.assertEquals(expectedShort, deserializedShort);
    }

    @Test
    public void testSimpleDateType() {
        Integer expectedDate = 17953;

        ByteBuffer serializedDate = SimpleDateType.instance.decompose(expectedDate);

        Object deserializedShort = CassandraTypeDeserializer.deserialize(SimpleDateType.instance, serializedDate);

        Assert.assertEquals(expectedDate, deserializedShort);
    }

    @Test
    public void testTimeType() {
        Long expectedTime = 30L;

        ByteBuffer serializedTime = TimeType.instance.decompose(expectedTime);

        Object deserializedTime = CassandraTypeDeserializer.deserialize(TimeType.instance, serializedTime);

        Assert.assertEquals(expectedTime, deserializedTime);
    }

    @Test
    public void testTimestampType() {
        Date timestamp = new Date();
        Long expectedLongTimestamp = timestamp.getTime();

        ByteBuffer serializedTimestamp = TimestampType.instance.decompose(timestamp);

        Object deserializedTimestamp = CassandraTypeDeserializer.deserialize(TimestampType.instance, serializedTimestamp);

        Assert.assertEquals(expectedLongTimestamp, deserializedTimestamp);
    }

    @Test
    public void testTimeUUIDType() {
        UUID timeUUID = UUID.randomUUID();
        String expectedFixedUUID = Values.convertToString(CassandraTypeKafkaSchemaBuilders.UUID_TYPE, UuidUtil.asBytes(timeUUID));

        ByteBuffer serializedTimeUUID = TimeUUIDType.instance.decompose(timeUUID);

        Object deserializedTimeUUID = CassandraTypeDeserializer.deserialize(TimeUUIDType.instance, serializedTimeUUID);

        Assert.assertEquals(expectedFixedUUID, deserializedTimeUUID);
    }

    @Test
    public void testTupleType() {
        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(2);
        innerAbstractTypes.add(AsciiType.instance);
        innerAbstractTypes.add(ShortType.instance);
        TupleType tupleType = new TupleType(innerAbstractTypes);

        String sourceTupleString = "foo:1";
        ByteBuffer serializedTuple = tupleType.fromString(sourceTupleString);

        Object deserializedTuple = CassandraTypeDeserializer.deserialize(tupleType, serializedTuple);
        Schema tupleSchema = CassandraTypeDeserializer.getSchemaBuilder(tupleType).build();
        Struct expectedTuple = new Struct(tupleSchema)
                .put("field1", "foo")
                .put("field2", (short) 1);

        Assert.assertEquals(expectedTuple, deserializedTuple);
    }

    @Test
    public void testUserType() {
        // this is slightly complicated, so we're testing in two parts:
        // first, explicitly test for schema correctness
        ByteBuffer expectedTypeName = ByteBuffer.wrap("FooType".getBytes(Charset.defaultCharset()));
        List<FieldIdentifier> expectedFieldIdentifiers = new ArrayList<>();
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("asciiField".getBytes(Charset.defaultCharset()))));
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("doubleField".getBytes(Charset.defaultCharset()))));
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("durationField".getBytes(Charset.defaultCharset()))));
        // testing duration to make sure that recursive deserialization works correctly
        List<AbstractType<?>> expectedFieldTypes = new ArrayList<>();
        expectedFieldTypes.add(AsciiType.instance);
        expectedFieldTypes.add(DoubleType.instance);
        expectedFieldTypes.add(DurationType.instance);
        UserType userType = new UserType("barspace",
                expectedTypeName,
                expectedFieldIdentifiers,
                expectedFieldTypes,
                true);

        Schema userSchema = CassandraTypeDeserializer.getSchemaBuilder(userType).build();

        long expectedNanoDuration = (30 + 2) * ChronoUnit.DAYS.getDuration().toNanos() + 3;

        Struct expectedUserTypeData = new Struct(userSchema)
                .put("asciiField", "foobar")
                .put("doubleField", 1.5d)
                .put("durationField", expectedNanoDuration);

        Map<String, Object> jsonObject = new HashMap<>(3);
        jsonObject.put("\"asciiField\"", "foobar");
        jsonObject.put("\"doubleField\"", 1.5d);
        jsonObject.put("\"durationField\"", DurationType.instance.getSerializer().toString(Duration.newInstance(1, 2, 3)));
        Term userTypeObject = userType.fromJSONObject(jsonObject);

        ByteBuffer buffer = userTypeObject.bindAndGet(QueryOptions.DEFAULT);

        ByteBuffer serializedUserTypeObject = userType.decompose(buffer);

        Object deserializedUserTypeObject = CassandraTypeDeserializer.deserialize(userType, serializedUserTypeObject);

        Assert.assertEquals(expectedUserTypeData, deserializedUserTypeObject);
    }

    @Test
    public void testUTF8Type() {
        String expectedUTF8 = "Fourscore and seven years ago";

        ByteBuffer serializedUTF8 = UTF8Type.instance.decompose(expectedUTF8);

        Object deserializedUTF8 = CassandraTypeDeserializer.deserialize(UTF8Type.instance, serializedUTF8);

        Assert.assertEquals(expectedUTF8, deserializedUTF8);
    }

    @Test
    public void testUUIDType() {
        UUID uuid = UUID.randomUUID();
        String expectedFixedUUID = Values.convertToString(CassandraTypeKafkaSchemaBuilders.UUID_TYPE, UuidUtil.asBytes(uuid));

        ByteBuffer serializedUUID = UUIDType.instance.decompose(uuid);

        Object deserializedUUID = CassandraTypeDeserializer.deserialize(UUIDType.instance, serializedUUID);

        Assert.assertEquals(expectedFixedUUID, deserializedUUID);
    }

    @Test
    public void testReversedType() {
        Date timestamp = new Date();
        Long expectedLongTimestamp = timestamp.getTime();

        ByteBuffer serializedTimestamp = TimestampType.instance.decompose(timestamp);

        ReversedType<?> reversedTimeStampType = ReversedType.getInstance(TimestampType.instance);

        Object deserializedTimestamp = CassandraTypeDeserializer.deserialize(reversedTimeStampType, serializedTimestamp);

        Assert.assertEquals(expectedLongTimestamp, deserializedTimestamp);
    }

    @Test
    public void testListUUIDType() {

        List<UUID> originalList = new ArrayList<>();
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        UUID uuid3 = UUID.randomUUID();
        originalList.add(uuid1);
        originalList.add(uuid2);
        originalList.add(uuid3);

        List<String> expectedList = new ArrayList<>();
        String expectedUuidStr1 = Values.convertToString(CassandraTypeKafkaSchemaBuilders.UUID_TYPE, UuidUtil.asBytes(uuid1));
        String expectedUuidStr2 = Values.convertToString(CassandraTypeKafkaSchemaBuilders.UUID_TYPE, UuidUtil.asBytes(uuid2));
        String expectedUuidStr3 = Values.convertToString(CassandraTypeKafkaSchemaBuilders.UUID_TYPE, UuidUtil.asBytes(uuid3));
        expectedList.add(expectedUuidStr1);
        expectedList.add(expectedUuidStr2);
        expectedList.add(expectedUuidStr3);

        ListType<UUID> frozenListType = ListType.getInstance(UUIDType.instance, false);
        ByteBuffer serializedList = frozenListType.decompose(originalList);
        Object deserializedList = CassandraTypeDeserializer.deserialize(frozenListType, serializedList);
        Assert.assertEquals(expectedList, deserializedList);
    }
}
