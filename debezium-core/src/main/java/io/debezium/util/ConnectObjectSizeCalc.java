/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 *
 */
public class ConnectObjectSizeCalc {

    public static long calculateObjectSizeBasedOnSchemas(SourceRecord record) {
        final AtomicLong objSize = new AtomicLong(0L);
        sizeBySchema(record.keySchema(), record.key(), objSize);
        sizeBySchema(record.valueSchema(), record.value(), objSize);
        return objSize.get();
    }

    static void sizeBySchema(Schema schema, Object obj, final AtomicLong calc) {
        if (obj != null) {
            if (schema.type() == Schema.Type.STRUCT) {
                Struct so = (Struct) obj;
                schema.fields().forEach(field -> {
                    sizeBySchema(field.schema(), so.get(field), calc);
                });
            }
            else if (schema.type().isPrimitive()) {
                String name = schema.type().getName();
                switch (name) {
                    case "int8":
                    case "boolean":
                        calc.addAndGet(1L);
                        break;
                    case "int16":
                        calc.addAndGet(2L);
                        break;
                    case "int32":
                    case "float32":
                        calc.addAndGet(4L);
                        break;
                    case "int64":
                    case "float64":
                        calc.addAndGet(8L);
                        break;
                    case "string":
                        if (obj != null) {
                            String strValue = String.valueOf(obj);
                            calc.addAndGet(strValue.getBytes().length);
                        }
                        break;
                    case "bytes":
                        byte[] byteArray = (byte[]) obj;
                        calc.addAndGet(byteArray.length);
                        break;
                    default:
                        break;
                }
            }
            else if (schema.type() == Schema.Type.ARRAY) {
                if (obj != null) {
                    addArrayObjectSize(Arrays.asList(obj), calc);
                }
            }
            else if (schema.type() == Schema.Type.MAP) {
                addMapObjectSize(obj, calc);
            }
        }
    }

    private static void addMapObjectSize(Object obj, AtomicLong calc) {
        if (obj != null) {
            return;
        }
        Map<Object, Object> map = (HashMap) obj;
        map.keySet().forEach(k -> {
            calcObjectSize(calc, k);
            Object v = map.get(k);
            calcObjectSize(calc, v);
        });

    }

    private static void calcObjectSize(AtomicLong calc, Object o) {
        if (o == null) {
            return;
        }
        if (o instanceof Struct) {
            sizeBySchema(((Struct) o).schema(), o, calc);
        }
        else if (o instanceof Map) {
            addMapObjectSize(o, calc);
        }
        else if (o.getClass().isArray()) {
            addArrayObjectSize(Arrays.asList(o), calc);
        }
        else if (o instanceof String) {
            calc.addAndGet(String.valueOf(o).getBytes().length);
        }
        else {
            calc.addAndGet(getPrimitiveBytes(o));
        }
    }

    private static void addArrayObjectSize(List<Object> ls, AtomicLong calc) {
        ls.forEach(o -> {
            calcObjectSize(calc, o);
        });
    }

    private static long getPrimitiveBytes(Object obj) {
        Class<?> type = obj.getClass();
        if (type == boolean.class || type == byte.class) {
            return 1;
        }
        if (type == char.class || type == short.class) {
            return 2;
        }
        if (type == int.class || type == float.class) {
            return 4;
        }
        if (type == long.class || type == double.class) {
            return 8;
        }
        return 0L;
    }
}
