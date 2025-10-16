/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Struct;

/**
 * Geometry attributes handling for Oracle SDO_GEOMETRY tests.
 *
 * @author Marcin Piatek
 */
public class SdoGeometryUtil {

    private static BigDecimal[] toBigDecimalArray(double[] array, int arraySize) {
        if (array == null) {
            return null;
        }
        BigDecimal[] result = new BigDecimal[Math.max(array.length, arraySize)];
        for (int i = 0; i < array.length; i++) {
            result[i] = (array[i] % 1 == 0) ? BigDecimal.valueOf((long) array[i]) : BigDecimal.valueOf(array[i]);
        }
        return result;
    }

    public static Object[] toSdoGeometryAttributes(int sdoGtype, int sdoSrid, double[] sdoPoint, double[] sdoElemInfo, double[] sdoOrdinates) {
        return new Object[]{
                BigDecimal.valueOf(sdoGtype),
                BigDecimal.valueOf(sdoSrid),
                toBigDecimalArray(sdoPoint, 3),
                toBigDecimalArray(sdoElemInfo, 0),
                toBigDecimalArray(sdoOrdinates, 0)
        };
    }

    public static Object[] toSdoGeometryAttributes(int sdoGtype, int sdoSrid, double[] sdoPoint) {
        return toSdoGeometryAttributes(sdoGtype, sdoSrid, sdoPoint, null, null);
    }

    public static Object[] toSdoGeometryAttributes(Struct struct) throws SQLException {
        return new Object[]{
                struct.getAttributes()[0],
                struct.getAttributes()[1],
                struct.getAttributes()[2] instanceof java.sql.Struct ? ((java.sql.Struct) struct.getAttributes()[2]).getAttributes() : null,
                struct.getAttributes()[3] instanceof java.sql.Struct ? ((java.sql.Struct) struct.getAttributes()[3]).getAttributes() : null,
                struct.getAttributes()[4] instanceof java.sql.Struct ? ((java.sql.Struct) struct.getAttributes()[4]).getAttributes() : null
        };
    }

}
