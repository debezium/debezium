/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.storage.ydb;

import tech.ydb.auth.NopAuthProvider;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;

public final class YdbTransport {

    private YdbTransport() {
    }

    public static GrpcTransport open(YdbCommonConfig cfg) {
        String conn = cfg.getEndpoint() + "?database=" + cfg.getDatabase();
        GrpcTransportBuilder b = GrpcTransport.forConnectionString(conn);
        if (cfg.getAuthUser() != null && !cfg.getAuthUser().isBlank()) {
            String pwd = cfg.getAuthPassword() == null ? "" : cfg.getAuthPassword();
            b.withAuthProvider(new StaticCredentials(cfg.getAuthUser(), pwd));
        }
        else {
            b.withAuthProvider(NopAuthProvider.INSTANCE);
        }
        return b.build();
    }
}