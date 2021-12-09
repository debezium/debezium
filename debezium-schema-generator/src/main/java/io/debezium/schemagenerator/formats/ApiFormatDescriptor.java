/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.schemagenerator.formats;

public interface ApiFormatDescriptor {

    String getId();

    String getName();

    String getVersion();

    String getDescription();
}
