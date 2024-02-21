/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded.freemarker;

import lombok.Getter;

@Getter
public class CreateUserModel {
    private final String userName;
    private final String password;

    public CreateUserModel(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }
}
