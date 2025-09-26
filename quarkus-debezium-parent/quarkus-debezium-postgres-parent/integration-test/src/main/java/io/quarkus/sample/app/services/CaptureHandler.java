/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app.services;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.runtime.Capturing;
import io.debezium.runtime.CapturingEvent;
import io.quarkus.sample.app.dto.Order;
import io.quarkus.sample.app.dto.Product;

@ApplicationScoped
public class CaptureHandler {

    private final ProductService productService;
    private final OrderService orderService;
    private final Logger logger = LoggerFactory.getLogger(CaptureHandler.class);

    @Inject
    public CaptureHandler(ProductService productService, OrderService orderService) {
        this.productService = productService;
        this.orderService = orderService;
    }

    @Capturing
    public void capture(CapturingEvent<SourceRecord> event) {
        productService.captured();
    }

    @Capturing(destination = "dbserver1.public.products")
    public void products(CapturingEvent<Product> event) {
        logger.info("getting a product event for destination {} from capturing id {}", event.destination(), event.engine());
        productService.add(event.record());
    }

    @Capturing(destination = "dbserver2.public.orders", engine = "alternative")
    public void orders(CapturingEvent<Order> event) {
        logger.info("getting a order event for destination {} from capturing id {}", event.destination(), event.engine());
        orderService.add(event.record());
    }
}
