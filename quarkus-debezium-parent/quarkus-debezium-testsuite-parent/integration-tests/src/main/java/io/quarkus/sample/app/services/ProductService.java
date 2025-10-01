/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.services;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.sample.app.dto.Product;

@ApplicationScoped
public class ProductService {
    private final List<Product> products = new ArrayList<>();
    private final AtomicBoolean invoked = new AtomicBoolean(false);

    public void captured() {
        invoked.set(true);
    }

    public boolean isInvoked() {
        return invoked.get();
    }

    public void add(Product product) {
        products.add(product);
    }

    public List<Product> products() {
        return products;
    }
}
