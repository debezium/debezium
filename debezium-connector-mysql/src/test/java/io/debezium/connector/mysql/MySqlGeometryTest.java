/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import org.junit.Test;

import mil.nga.wkb.geom.Point;

import static org.junit.Assert.assertEquals;

/**
 * @author Omar Al-Safi
 */
public class MySqlGeometryTest {

    private MySqlGeometry mySqlGeometry;

    @Test
    public void shouldConvertMySqlBytesToPoint() throws Exception {
        byte[] mysqlBytes = {
            0, 0, 0, 0, 1, 1, 0, 0, 0, -29, -91, -101, -60, 32, -16, 27, 64, 21, -95, 67, -90, -99, 56, 50, 64
        }; //This represents 'POINT(6.9845 18.22115554)'
        mySqlGeometry = MySqlGeometry.fromBytes(mysqlBytes);
        assertPoint(6.9845, 18.22115554, mySqlGeometry.getPoint());
    }

    protected void assertPoint(double x, double y, Point point) {
        assertEquals(x, point.getX(), 0.0001);
        assertEquals(y, point.getY(), 0.0001);
    }
}
