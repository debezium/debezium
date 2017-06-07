/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import org.postgresql.core.Oid;
import org.postgresql.util.PSQLException;

import io.debezium.relational.Column;

/**
 * Extension to the {@link org.postgresql.core.Oid} class which contains Postgres specific datatypes not found currently in the
 * JDBC driver implementation classes.
 * 
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public final class PgOid extends Oid {
    
    /**
     * A list of PG types not known by the JDBC driver atm.
     */
    protected static final int JSONB_JDBC_OID = 1111;
    
    /**
     * Internal PG types as returned by the plugin
     */
    protected static final int JSONB_OID = 3802;

    protected static final int TSTZRANGE_OID = 3910;
    
    private PgOid() {
    }
    
    protected static int jdbcColumnToOid(Column column) {
        String typeName = column.typeName();
        if (typeName.toUpperCase().equals("TSTZRANGE")) {
            return TSTZRANGE_OID;
        }
        try {
            return valueOf(typeName);
        } catch (PSQLException e) {
            // not known by the driver PG driver
            return column.jdbcType();
        }
    }
}
