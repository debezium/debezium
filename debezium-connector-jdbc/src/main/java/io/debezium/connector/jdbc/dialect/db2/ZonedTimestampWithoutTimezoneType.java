/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.db2;

import java.time.ZonedDateTime;

import org.hibernate.query.BindableType;
import org.hibernate.type.StandardBasicTypes;

import io.debezium.connector.jdbc.type.Type;
import io.debezium.connector.jdbc.type.debezium.ZonedTimestampType;
import io.debezium.time.ZonedTimestamp;

/**
 * An implementation of {@link Type} for {@link ZonedTimestamp} values.
 *
 * @author Chris Cranford
 */
class ZonedTimestampWithoutTimezoneType extends ZonedTimestampType {

    public static final ZonedTimestampWithoutTimezoneType INSTANCE = new ZonedTimestampWithoutTimezoneType();

    @Override
    protected BindableType<ZonedDateTime> getBindableType() {
        return StandardBasicTypes.ZONED_DATE_TIME_WITHOUT_TIMEZONE;
    }

}
