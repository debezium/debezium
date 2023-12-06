/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.databases.mongodb.sharded.componentfactories.OcpShardModelFactory;
import io.debezium.testing.system.tools.databases.mongodb.sharded.freemarkermodels.CreateUserModel;
import io.fabric8.kubernetes.api.model.apps.Deployment;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

public class MongoShardedUtil {
    private static Configuration configuration;

    public static Configuration getFreemarkerConfiguration() {
        if (configuration == null) {
            configuration = new Configuration(Configuration.VERSION_2_3_32);
            try {
                configuration.setDirectoryForTemplateLoading(new File("src/test/resources/database-resources/mongodb/sharded/command-templates"));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return configuration;
    }

    public static List<Integer> intRange(int count) {
        return IntStream.rangeClosed(0, count - 1).boxed().collect(Collectors.toList());
    }

    public static OpenShiftUtils.CommandOutputs executeMongoShOnPod(OpenShiftUtils ocpUtils, String project, Deployment deployment, String connectionString,
                                                                    String command, boolean debugLogs) {
        try {
            return ocpUtils.executeCommand(deployment, project, debugLogs,
                    "mongosh",
                    connectionString,
                    "--eval",
                    command);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Failed executing mongosh command", e);
        }
    }

    public static OpenShiftUtils.CommandOutputs executeMongoShOnPod(OpenShiftUtils ocpUtils, String project, Deployment deployment, int port, String command,
                                                                    boolean debugLogs) {
        try {
            return ocpUtils.executeCommand(deployment, project, debugLogs,
                    "mongosh",
                    String.format("localhost:%d/admin", port),
                    "--eval",
                    command);
        }
        catch (InterruptedException e) {
            throw new RuntimeException("Failed executing mongosh command", e);
        }
    }

    public static String createDebeziumUserCommand(String userName, String password) throws IOException, TemplateException {
        var writer = new StringWriter();
        Template template = getFreemarkerConfiguration().getTemplate(OcpMongoShardedConstants.CREATE_DBZ_USER_TEMPLATE);
        template.process(new CreateUserModel(userName, password), writer);
        return writer.toString();
    }

    public static List<MongoShardKey> getTestShardKeys() {
        MongoShardKey customersKey = new MongoShardKey("inventory.customers", "_id", MongoShardKey.ShardingType.NORMAL);
        customersKey.getKeyRanges().add(new ShardKeyRange(OcpShardModelFactory.getShardReplicaSetName(1), "1000", "1003"));
        customersKey.getKeyRanges().add(new ShardKeyRange(OcpShardModelFactory.getShardReplicaSetName(2), "1003", "1004"));

        MongoShardKey productsKey = new MongoShardKey("inventory.products", "_id", MongoShardKey.ShardingType.HASHED);
        return List.of(customersKey, productsKey);
    }

    public static String createRootUserCommand(String userName, String password) {
        return "db.getSiblingDB('admin').createUser({user: '" + userName + "', pwd: '" + password + "', roles: [{role:\"root\",db:\"admin\"}] })";
    }

    public static void addKeyFileToDeployment(Deployment deployment) {
        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getCommand()
                .addAll(List.of("--keyFile", OcpMongoShardedConstants.KEYFILE_PATH_IN_CONTAINER));
    }
}
