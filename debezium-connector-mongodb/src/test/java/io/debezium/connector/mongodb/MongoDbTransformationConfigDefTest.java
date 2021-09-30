/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import io.debezium.config.TransformationConfigDefinitionMetadataTest;
import io.debezium.connector.mongodb.transforms.ExtractNewDocumentState;

public class MongoDbTransformationConfigDefTest extends TransformationConfigDefinitionMetadataTest {

    public MongoDbTransformationConfigDefTest() {
        super(new ExtractNewDocumentState<>());
    }
}
