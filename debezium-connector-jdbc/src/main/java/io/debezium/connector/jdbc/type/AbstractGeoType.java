/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.util.Base64;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.hibernate.query.Query;

public abstract class AbstractGeoType extends AbstractType {
    public static final String SRID = "srid";
    public static final String WKB = "wkb";

    @Override
    public int bind(Query<?> query, int index, Schema schema, Object value) {

        if (value == null) {
            query.setParameter(index, null);
            return 1;
        }

        if (value instanceof Struct) {
            final int srid = ((Struct) value).getInt32(SRID);
            final byte[] wkb = ((Struct) value).getBytes(WKB);

            query.setParameter(index, Base64.getDecoder().decode(wkb));
            query.setParameter(index + 1, srid);
            return 2;
        }

        throwUnexpectedValue(value);
        return 0;
    }
}
