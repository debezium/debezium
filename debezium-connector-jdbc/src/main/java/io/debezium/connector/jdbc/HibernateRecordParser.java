/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.HibernateJaxbRecordMapper.getHibernateAttributeType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.Session;
import org.hibernate.type.BigIntegerType;
import org.hibernate.type.BooleanType;
import org.hibernate.type.ByteType;
import org.hibernate.type.FloatType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.TimeType;
import org.hibernate.type.TimestampType;

/** Functionalities to convert {@link SinkRecord#value()}/{@link SinkRecord#key()} to a proper object
 *  that can be use in Hibernate ORM
 *
 */
public class HibernateRecordParser {

    /** Create dataMap from field name to its data from {@link SinkRecord} with record Data Maps made by
     *
     * @return a Map from {@link Field#name()} to an Object that can be use on {@link Session#save(String, Object)}
     */
    public static Map<String, Object> getDataMap(Struct struct) throws Exception {
        Map<String, Object> dataMap = new HashMap<>();

        for (Field field : struct.schema().fields()) {
            String stringData = String.valueOf(struct.get(field));

            Object data = null;
            if (stringData != null) {
                switch (getHibernateAttributeType(field.schema())) {
                    case "integer":
                        data = IntegerType.INSTANCE.stringToObject(stringData);
                        break;
                    case "big_integer":
                        data = BigIntegerType.INSTANCE.stringToObject(stringData);
                        break;
                    case "long":
                        data = LongType.INSTANCE.stringToObject(stringData);
                        break;
                    case "float":
                        data = FloatType.INSTANCE.fromStringValue(stringData);
                        break;
                    case "boolean":
                        data = BooleanType.INSTANCE.stringToObject(stringData);
                        break;
                    case "byte":
                        data = ByteType.INSTANCE.stringToObject(stringData);
                        break;
                    case "timestamp":
                        data = TimestampType.INSTANCE.fromStringValue(stringData);
                        break;
                    case "time":
                        data = TimeType.INSTANCE.fromStringValue(stringData);
                        break;
                    case "date":
                        data = Date.valueOf(LocalDate.ofEpochDay(Long.parseLong(stringData)));
                        break;
                    default:
                        data = stringData;
                }
            }
            dataMap.put(
                    field.name(),
                    data);
        }

        return dataMap;
    }
}
