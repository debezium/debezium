/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.quarkus.sample.app.resources;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import io.quarkus.sample.app.services.OrderService;
import io.quarkus.sample.app.services.ProductService;

@Path("/captured")
@ApplicationScoped
public class CapturingResource {

    private final ProductService productService;
    private final OrderService orderService;

    public CapturingResource(ProductService productService, OrderService orderService) {
        this.productService = productService;
        this.orderService = orderService;
    }

    @GET
    @Path("all")
    public Response captured() {
        if (productService.isInvoked()) {
            return Response.status(Response.Status.FOUND).build();
        }

        return Response.status(Response.Status.NOT_FOUND).build();
    }

    @GET
    @Path("products")
    public Response products() {
        if (productService.products().isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(productService.products()).build();
    }

    @GET
    @Path("orders")
    public Response orders() {
        if (orderService.orders().isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        return Response.ok(orderService.orders()).build();
    }

}
