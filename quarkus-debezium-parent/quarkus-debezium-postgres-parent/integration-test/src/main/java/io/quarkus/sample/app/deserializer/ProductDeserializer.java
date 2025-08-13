/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.deserializer;

import io.quarkus.debezium.engine.deserializer.ObjectMapperDeserializer;
import io.quarkus.sample.app.Product;

public class ProductDeserializer extends ObjectMapperDeserializer<Product> {
    public ProductDeserializer() {
        super(Product.class);
    }
}
