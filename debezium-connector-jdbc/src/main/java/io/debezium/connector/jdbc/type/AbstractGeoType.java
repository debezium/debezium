/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.type;

import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
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
            // Default srid is 0 for both
            // MySQL https://dev.mysql.com/doc/refman/8.0/en/spatial-reference-systems.html#:~:text=The%20SRS%20denoted%20in%20MySQL,for%20spatial%20data%20in%20MySQL.
            // PostgreSQL https://postgis.net/docs/using_postgis_dbmanagement.html#spatial_ref_sys_table
            final Integer srid = Optional.ofNullable(((Struct) value).getInt32(SRID)).orElse(0);
            final byte[] wkb = ((Struct) value).getBytes(WKB);

            query.setParameter(index, wkb);
            query.setParameter(index + 1, srid);
            return 2;
        }

        throw new ConnectException(String.format("Unexpected %s value '%s' with type '%s'", getClass().getSimpleName(),
                value, value.getClass().getName()));
    }
}
