/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import io.debezium.data.Bits;
import io.debezium.time.Date;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTime;
import io.debezium.time.ZonedTimestamp;

class HibernateJaxbRecordMapperTest {

    @Test
    void getHibernateAttributeWitPrimitiveTypeTest() {
        Assert.assertEquals("integer", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.INT8_SCHEMA));

        Assert.assertEquals("big_integer", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.INT16_SCHEMA));
        Assert.assertEquals("big_integer", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.INT32_SCHEMA));
        Assert.assertEquals("big_integer", HibernateJaxbRecordMapper.getHibernateAttributeType(MicroDuration.schema()));

        Assert.assertEquals("long", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.INT64_SCHEMA));

        Assert.assertEquals("float", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.FLOAT32_SCHEMA));
        Assert.assertEquals("float", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.FLOAT64_SCHEMA));

        Assert.assertEquals("boolean", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.BOOLEAN_SCHEMA));

        Assert.assertEquals("byte", HibernateJaxbRecordMapper.getHibernateAttributeType(Bits.schema(10)));

        Assert.assertEquals("timestamp", HibernateJaxbRecordMapper.getHibernateAttributeType(ZonedTimestamp.schema()));

        Assert.assertEquals("time", HibernateJaxbRecordMapper.getHibernateAttributeType(ZonedTime.schema()));

        Assert.assertEquals("date", HibernateJaxbRecordMapper.getHibernateAttributeType(Date.schema()));

        Assert.assertEquals("text", HibernateJaxbRecordMapper.getHibernateAttributeType(Schema.STRING_SCHEMA));
    }

    @Test
    void getHibernateAttributeWitNonPrimitiveTypeTest() {
        Assert.assertEquals(
                "text",
                HibernateJaxbRecordMapper.getHibernateAttributeType(SchemaBuilder.struct().build()));

        Assert.assertEquals(
                "text",
                HibernateJaxbRecordMapper.getHibernateAttributeType(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build()));

        Assert.assertEquals(
                "text",
                HibernateJaxbRecordMapper.getHibernateAttributeType(SchemaBuilder.array(Schema.STRING_SCHEMA).build()));
    }
}
