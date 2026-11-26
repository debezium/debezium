/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.jev;

import io.debezium.ai.decision.systemone.SystemOneDecisionServiceFactory;

/**
 * System One decision service provider for Jev.
 *
 * @author Jiri Pechanec
 */
public class JevDecisionServiceFactory extends SystemOneDecisionServiceFactory {

    public JevDecisionServiceFactory() {
        super("jev", "Jev", "https://api.typesafe.ai", "jev-latest", true, true, 10);
    }
}
