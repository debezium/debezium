/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.transforms;

import io.debezium.config.TransformationConfigDefinitionMetadataTest;

public class ScriptingTransformationConfigDefTest extends TransformationConfigDefinitionMetadataTest {

    public ScriptingTransformationConfigDefTest() {
        super(new Filter<>(), new ContentBasedRouter<>());
    }
}
