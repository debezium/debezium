/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.sample.app;

import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CaptureService {
    private final AtomicBoolean invoked = new AtomicBoolean(false);

    public void capture() {
        invoked.set(true);
    }

    public boolean isInvoked() {
        return invoked.get();
    }
}
