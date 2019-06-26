/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import com.datastax.driver.core.DataType;
import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class UserTypeConverter implements TypeConverter<UserType> {

    @Override
    public UserType convert(DataType dataType) {
        com.datastax.driver.core.UserType userType = (com.datastax.driver.core.UserType) dataType;
        List<DataType> innerTypes = dataType.getTypeArguments();
        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(innerTypes.size());
        for (DataType dt: innerTypes) {
            innerAbstractTypes.add(CassandraTypeConverter.convert(dt));
        }

        String typeNameString = userType.getTypeName();
        Collection<String> fieldNames = userType.getFieldNames();

        ByteBuffer typeNameBuffer = UTF8Type.instance.fromString(typeNameString);

        List<FieldIdentifier> fieldIdentifiers = new ArrayList<>(fieldNames.size());
        for (String fieldName: fieldNames) {
            fieldIdentifiers.add(FieldIdentifier.forInternalString(fieldName));
        }

        return new UserType(userType.getKeyspace(),
                            typeNameBuffer,
                            fieldIdentifiers,
                            innerAbstractTypes,
                            !userType.isFrozen());
    }

}
