/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.redis;

public class RedisClientConnectionException extends RuntimeException {

    private static final long serialVersionUID = -4315965419500005492L;

    public RedisClientConnectionException(Throwable cause) {
        super(cause);
    }

}
