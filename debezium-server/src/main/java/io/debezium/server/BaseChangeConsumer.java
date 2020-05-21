/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server;

import javax.annotation.PostConstruct;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic services provided to all change consumers.
 *
 * @author Jiri Pechanec
 *
 */
public class BaseChangeConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseChangeConsumer.class);

    protected StreamNameMapper streamNameMapper = (x) -> x;

    @Inject
    Instance<StreamNameMapper> customStreamNameMapper;

    @PostConstruct
    void init() {
        if (customStreamNameMapper.isResolvable()) {
            streamNameMapper = customStreamNameMapper.get();
        }
        LOGGER.info("Using '{}' stream name mapper", streamNameMapper);
    }
}