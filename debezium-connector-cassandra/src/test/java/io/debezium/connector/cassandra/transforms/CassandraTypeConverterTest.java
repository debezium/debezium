/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.FieldIdentifier;
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
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;

public class CassandraTypeConverterTest {

    @Test
    public void testAscii() {
        DataType asciiType = DataType.ascii();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(asciiType);

        AsciiType expectedType = AsciiType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testBlob() {
        DataType blobType = DataType.blob();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(blobType);

        BytesType expectedType = BytesType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testBigInt() {
        DataType bigIntType = DataType.bigint();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(bigIntType);

        LongType expectedType = LongType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testBoolean() {
        DataType booleanType = DataType.cboolean();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(booleanType);

        BooleanType expectedType = BooleanType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testCounter() {
        DataType counterType = DataType.counter();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(counterType);

        CounterColumnType expectedType = CounterColumnType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDate() {
        DataType dateType = DataType.date();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(dateType);

        SimpleDateType expectedType = SimpleDateType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDecimal() {
        DataType decimalType = DataType.decimal();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(decimalType);

        DecimalType expectedType = DecimalType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDouble() {
        DataType doubleType = DataType.cdouble();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(doubleType);

        DoubleType expectedType = DoubleType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDuration() {
        DataType durationType = DataType.duration();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(durationType);

        DurationType expectedType = DurationType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testFloat() {
        DataType floatType = DataType.cfloat();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(floatType);

        FloatType expectedType = FloatType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testInet() {
        DataType inetType = DataType.inet();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(inetType);

        InetAddressType expectedType = InetAddressType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testInt() {
        DataType intType = DataType.cint();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(intType);

        Int32Type expectedType = Int32Type.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testList() {
        // list of ints
        // test non-frozen
        DataType listType = DataType.list(DataType.cint(), false);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(listType);

        ListType<?> expectedType = ListType.getInstance(Int32Type.instance, true);
        Assert.assertEquals(expectedType, convertedType);

        // test frozen
        listType = DataType.list(DataType.cint(), true);
        convertedType = CassandraTypeConverter.convert(listType);
        expectedType = ListType.getInstance(Int32Type.instance, false);
        Assert.assertEquals(expectedType, convertedType);
        Assert.assertTrue("Expected convertedType to be frozen", convertedType.isFrozenCollection());
    }

    @Test
    public void testMap() {
        // map from ASCII to Double
        // test non-frozen
        DataType mapType = DataType.map(DataType.ascii(), DataType.cdouble());
        AbstractType<?> convertedType = CassandraTypeConverter.convert(mapType);

        MapType<?, ?> expectedType = MapType.getInstance(AsciiType.instance, DoubleType.instance, true);
        Assert.assertEquals(expectedType, convertedType);

        // test frozen
        mapType = DataType.map(DataType.ascii(), DataType.cdouble(), true);
        convertedType = CassandraTypeConverter.convert(mapType);
        expectedType = MapType.getInstance(AsciiType.instance, DoubleType.instance, false);
        Assert.assertEquals(expectedType, convertedType);
        Assert.assertTrue("Expected convertType to be frozen", convertedType.isFrozenCollection());
    }

    @Test
    public void testSet() {
        // set of floats
        // test non-frozen
        DataType setType = DataType.set(DataType.cfloat(), false);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(setType);

        SetType<?> expectedType = SetType.getInstance(FloatType.instance, true);
        Assert.assertEquals(expectedType, convertedType);

        // test frozen
        setType = DataType.set(DataType.cfloat(), true);
        convertedType = CassandraTypeConverter.convert(setType);
        expectedType = SetType.getInstance(FloatType.instance, false);
        Assert.assertEquals(expectedType, convertedType);
        Assert.assertTrue("Expected convertedType to be frozen", convertedType.isFrozenCollection());

    }

    @Test
    public void testSmallInt() {
        DataType smallIntType = DataType.smallint();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(smallIntType);

        ShortType expectedType = ShortType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testText() {
        DataType textType = DataType.text();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(textType);

        UTF8Type expectedType = UTF8Type.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTime() {
        DataType timeType = DataType.time();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(timeType);

        TimeType expectedType = TimeType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTimestamp() {
        DataType timestampType = DataType.timestamp();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(timestampType);

        TimestampType expectedType = TimestampType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTimeUUID() {
        DataType timeUUID = DataType.timeuuid();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(timeUUID);

        TimeUUIDType expectedType = TimeUUIDType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTinyInt() {
        DataType tinyInt = DataType.tinyint();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(tinyInt);

        ByteType expectedType = ByteType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTuple() {
        // tuple containing timestamp and smallint.
        // tuples are always frozen, so we don't need to test that.
        // we don't care about the protocol version or the codec registry.
        DataType tupleType = TupleType.of(null, null, DataType.timestamp(), DataType.smallint());
        AbstractType<?> convertedType = CassandraTypeConverter.convert(tupleType);

        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(2);
        innerAbstractTypes.add(TimestampType.instance);
        innerAbstractTypes.add(ShortType.instance);
        org.apache.cassandra.db.marshal.TupleType expectedType = new org.apache.cassandra.db.marshal.TupleType(innerAbstractTypes);

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testUdt() {
        // we can't create a UserType directly, and there isn't really a good way to make one some other way, so...
        // mock it!
        UserType userType = Mockito.mock(UserType.class);
        Mockito.when(userType.getName()).thenReturn(DataType.Name.UDT);
        Mockito.when(userType.getTypeName()).thenReturn("FooType");
        Mockito.when(userType.getKeyspace()).thenReturn("barspace");
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("asciiField");
        fieldNames.add("doubleField");
        Mockito.when(userType.getFieldNames()).thenReturn(fieldNames);
        Mockito.when(userType.getFieldType("asciiField")).thenReturn(DataType.ascii());
        Mockito.when(userType.getFieldType("doubleField")).thenReturn(DataType.cdouble());
        Mockito.when(userType.isFrozen()).thenReturn(false, true); // cheaty way to test non-frozen and then frozen path.

        ByteBuffer expectedTypeName = ByteBuffer.wrap("FooType".getBytes(Charset.defaultCharset()));
        List<FieldIdentifier> expectedFieldIdentifiers = new ArrayList<>();
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("asciiField".getBytes(Charset.defaultCharset()))));
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("doubleField".getBytes(Charset.defaultCharset()))));
        List<AbstractType<?>> expectedFieldTypes = new ArrayList<>();
        expectedFieldTypes.add(AsciiType.instance);
        expectedFieldTypes.add(DoubleType.instance);

        // non-frozen
        org.apache.cassandra.db.marshal.UserType expectedAbstractType = new org.apache.cassandra.db.marshal.UserType("barspace",
                expectedTypeName,
                expectedFieldIdentifiers,
                expectedFieldTypes,
                true);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(userType);
        Assert.assertEquals(expectedAbstractType, convertedType);

        // frozen
        expectedAbstractType = new org.apache.cassandra.db.marshal.UserType("barspace",
                expectedTypeName,
                expectedFieldIdentifiers,
                expectedFieldTypes,
                false);
        convertedType = CassandraTypeConverter.convert(userType);
        Assert.assertEquals(expectedAbstractType, convertedType);
    }

    @Test
    public void testUUID() {
        DataType uuid = DataType.uuid();
        AbstractType<?> convertedType = CassandraTypeConverter.convert(uuid);

        UUIDType expectedType = UUIDType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }
}
