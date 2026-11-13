/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink.eventhandler.relational;

import org.bson.BsonDocument;
import org.bson.BsonObjectId;

import io.debezium.sink.naming.ColumnNamingStrategy;

public final class RelationalEventHandler {

    private static final String ID_FIELD = "_id";

    private RelationalEventHandler() {
    }

    public static BsonDocument generateFilterDoc(final BsonDocument keyDoc, ColumnNamingStrategy columnNamingStrategy) {
        if (keyDoc.keySet().isEmpty()) {
            return new BsonDocument(ID_FIELD, new BsonObjectId());
        }
        BsonDocument pk = new BsonDocument();
        for (String f : keyDoc.keySet()) {
            pk.put(columnNamingStrategy.resolveColumnName(f), keyDoc.get(f));
        }
        return new BsonDocument(ID_FIELD, pk);
    }

    public static BsonDocument generateUpsertOrReplaceDoc(final BsonDocument keyDoc, final BsonDocument afterDoc, final BsonDocument filterDoc,
                                                          ColumnNamingStrategy columnNamingStrategy) {

        BsonDocument upsertDoc = new BsonDocument();
        if (filterDoc.containsKey(ID_FIELD)) {
            upsertDoc.put(ID_FIELD, filterDoc.get(ID_FIELD));
        }

        if (afterDoc != null) {
            for (String f : afterDoc.keySet()) {
                if (!keyDoc.containsKey(f)) {
                    upsertDoc.put(columnNamingStrategy.resolveColumnName(f), afterDoc.get(f));
                }
            }
        }
        return upsertDoc;
    }
}
