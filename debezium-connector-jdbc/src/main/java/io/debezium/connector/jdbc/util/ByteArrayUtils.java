/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.util;

import java.nio.ByteBuffer;

import io.debezium.util.HexConverter;

/**
 * @author Chris Cranford
 */
public class ByteArrayUtils {

    public static String getByteArrayAsHex(Object value) {
        return HexConverter.convertToHexString(getByteArrayFromValue(value));
    }

    public static byte[] getByteArrayFromValue(Object value) {
        final byte[] byteArray;
        if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) value).slice();
            byteArray = new byte[buffer.remaining()];
            buffer.get(byteArray);
        }
        else {
            byteArray = (byte[]) value;
        }
        return byteArray;
    }

}
