/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.junit.Test;

public class JsonSerializationTest {

    private JsonSerialization serialization = new JsonSerialization();

    @Test
    public void shouldGeOnlyIdFromCompositeKey() {
        var id = new BsonInt32(42);
        var composite = new BsonDocument("email", new BsonString("foo@bar.com"))
                .append("_id", id);

        var key = serialization.getDocumentIdChangeStream(composite);

        Assertions.assertThat(key).isEqualTo("42");
    }

    @Test
    public void shouldGetEqualDocumentIdFromSimpleAndComposite() {
        var id = new BsonObjectId();
        var simple = new BsonDocument("_id", id);
        var composite = new BsonDocument("email", new BsonString("foo@bar.com"))
                .append("_id", id);

        var simpleKey = serialization.getDocumentIdChangeStream(simple);
        var compositeKey = serialization.getDocumentIdChangeStream(composite);

        Assertions.assertThat(compositeKey).isEqualTo(simpleKey);
    }

}
