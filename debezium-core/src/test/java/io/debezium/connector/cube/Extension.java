/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cube;

import org.jboss.arquillian.core.spi.LoadableExtension;
import org.jboss.arquillian.test.spi.TestEnricher;

/**
 * Register {@link CubeReference} extension in Arquillian
 *
 * @author Jiri Pechanec
 *
 */
public class Extension implements LoadableExtension {

    @Override
    public void register(final ExtensionBuilder builder) {
        builder.service(TestEnricher.class, DatabaseCubeTestEnricher.class);
    }

}
