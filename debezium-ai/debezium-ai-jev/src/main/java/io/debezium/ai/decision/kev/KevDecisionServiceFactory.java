/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ai.decision.kev;

import io.debezium.ai.decision.systemone.SystemOneDecisionServiceFactory;

/**
 * System One decision service provider for Kev.
 *
 * @author Jiri Pechanec
 */
public class KevDecisionServiceFactory extends SystemOneDecisionServiceFactory {

    public KevDecisionServiceFactory() {
        super("kev", "Kev", "http://127.0.0.1:8009", "kev-latest", false, false, 255);
    }
}
