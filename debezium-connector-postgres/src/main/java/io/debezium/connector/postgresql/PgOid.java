/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import org.postgresql.core.Oid;

/**
 * Extension to the {@link org.postgresql.core.Oid} class which contains Postgres specific datatypes not found currently in the
 * JDBC driver implementation classes.
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public final class PgOid extends Oid {

    /**
     * Internal PG types as returned by the plugin
     */
    public static final int JSONB_OID = 3802;
    public static final int TSRANGE_OID = 3908;
    public static final int TSRANGE_ARRAY = 3909;
    public static final int TSTZRANGE_OID = 3910;
    public static final int TSTZRANGE_ARRAY = 3911;
    public static final int DATERANGE_OID = 3912;
    public static final int DATERANGE_ARRAY = 3913;
    public static final int INET_OID = 869;
    public static final int INET_ARRAY = 1041;
    public static final int CIDR_OID = 650;
    public static final int CIDR_ARRAY = 651;
    public static final int MACADDR_OID = 829;
    public static final int MACADDR_ARRAY = 1040;
    public static final int MACADDR8_OID = 774;
    public static final int MACADDR8_ARRAY = 775;
    public static final int INT4RANGE_OID = 3904;
    public static final int INT4RANGE_ARRAY = 3905;
    public static final int NUM_RANGE_OID = 3906;
    public static final int NUM_RANGE_ARRAY = 3907;
    public static final int INT8RANGE_OID = 3926;
    public static final int INT8RANGE_ARRAY = 3927;
    public static final int TSVECTOR_OID = 3614;
}
