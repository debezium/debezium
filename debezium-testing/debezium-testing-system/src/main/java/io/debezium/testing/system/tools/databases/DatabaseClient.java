/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases;

/**
 *
 * @author Jakub Cechacek
 */
public interface DatabaseClient<C, E extends Throwable> {

    void execute(Commands<C, E> commands) throws E;
}
