/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.databases.mongodb.sharded.componentproviders.OcpShardModelProvider;
import io.debezium.testing.system.tools.databases.mongodb.sharded.freemarker.CreateUserModel;
import io.debezium.testing.system.tools.databases.mongodb.sharded.freemarker.FreemarkerConfiguration;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;

import freemarker.template.Template;
import freemarker.template.TemplateException;

public class MongoShardedUtil {

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

    public static String createPasswordUserCommand(String userName, String password) throws IOException, TemplateException {
        var writer = new StringWriter();
        Template template = new FreemarkerConfiguration().getFreemarkerConfiguration().getTemplate(OcpMongoShardedConstants.CREATE_DBZ_USER_TEMPLATE);
        template.process(new CreateUserModel(userName, password), writer);
        return writer.toString();
    }

    /**
     * Get command that creates a user (with correct permissions for debezium usage) in mongodb for x509 client authentication.
     * @param subjectName must match the subject of certificate used to authenticate user
     * @return mongo command string
     * @throws IOException
     * @throws TemplateException
     */
    public static String createCertUserCommand(String subjectName) throws IOException, TemplateException {
        var writer = new StringWriter();
        Template template = new FreemarkerConfiguration().getFreemarkerConfiguration().getTemplate(OcpMongoShardedConstants.CREATE_CERT_USER_TEMPLATE);
        template.process(new CreateUserModel(subjectName, ""), writer);
        return writer.toString();
    }

    public static List<MongoShardKey> getTestShardKeys() {
        MongoShardKey customersKey = new MongoShardKey("inventory.customers", "_id", MongoShardKey.ShardingType.RANGED);
        customersKey.getKeyRanges().add(new ShardKeyRange(OcpShardModelProvider.getShardReplicaSetName(1), "1000", "1003"));
        customersKey.getKeyRanges().add(new ShardKeyRange(OcpShardModelProvider.getShardReplicaSetName(2), "1003", "1004"));

        MongoShardKey productsKey = new MongoShardKey("inventory.products", "_id", MongoShardKey.ShardingType.HASHED);
        return List.of(customersKey, productsKey);
    }

    public static String createRootUserCommand(String userName, String password) {
        return "db.getSiblingDB('admin').createUser({user: '" + userName + "', pwd: '" + password + "', roles: [{role:\"root\",db:\"admin\"}] })";
    }

    /**
     * Modify the mongodb deployment to use keyfile for internal authentication.
     * Mutually exclusive with using x509 certificates for internal auth
     * @param deployment
     */
    public static void addKeyFileToDeployment(Deployment deployment) {
        final String keyFileVolume = "keyfile-vol";

        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getVolumes()
                .add(new VolumeBuilder()
                        .withName(keyFileVolume)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                .withDefaultMode(0600)
                                .withName(OcpMongoShardedConstants.KEYFILE_CONFIGMAP_NAME)
                                .build())
                        .build());
        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getVolumeMounts()
                .add(new VolumeMountBuilder()
                        .withName(keyFileVolume)
                        .withMountPath(OcpMongoShardedConstants.KEYFILE_LOCATION_IN_CONTAINER)
                        .build());
        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getCommand()
                .addAll(List.of("--keyFile", OcpMongoShardedConstants.KEYFILE_LOCATION_IN_CONTAINER + OcpMongoShardedConstants.KEYFILE_FILENAME_IN_CONTAINER));

    }

    /**
     * Modify the mongodb deployment object to mount server and ca certificates (from configMap, name is set by constant) and use them
     * in mongodb deployment for internal and client authentication.
     * Mutually exclusive with using keyfile for internal auth
     * @param deployment
     */
    public static void addCertificatesToDeployment(Deployment deployment) {
        String caCertVolume = "ca-cert-volume";
        String serverCertVolume = "server-cert-volume";
        String volumeMountRootPath = "/opt/";

        // volumes
        deployment.getSpec()
                .getTemplate()
                .getSpec()
                .getVolumes()
                .add(new VolumeBuilder()
                        .withName(serverCertVolume)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                .withName(OcpMongoCertGenerator.SERVER_CERT_CONFIGMAP)
                                .build())
                        .build());
        deployment.getSpec()
                .getTemplate()
                .getSpec()
                .getVolumes()
                .add(new VolumeBuilder()
                        .withName(caCertVolume)
                        .withConfigMap(new ConfigMapVolumeSourceBuilder()
                                .withName(OcpMongoCertGenerator.CA_CERT_CONFIGMAP)
                                .build())
                        .build());

        // volume mounts
        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getVolumeMounts()
                .add(new VolumeMountBuilder()
                        .withName(serverCertVolume)
                        .withMountPath(volumeMountRootPath + OcpMongoCertGenerator.SERVER_CERT_CONFIGMAP)
                        .build());
        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getVolumeMounts()
                .add(new VolumeMountBuilder()
                        .withName(caCertVolume)
                        .withMountPath(volumeMountRootPath + OcpMongoCertGenerator.CA_CERT_CONFIGMAP)
                        .build());

        // command
        deployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getCommand()
                .addAll(List.of(
                        "--clusterAuthMode", "x509",
                        "--tlsMode", "preferTLS",
                        "--tlsCertificateKeyFile", volumeMountRootPath + OcpMongoCertGenerator.SERVER_CERT_CONFIGMAP + "/" + OcpMongoCertGenerator.SERVER_CERT_SUBPATH,
                        "--tlsCAFile", volumeMountRootPath + OcpMongoCertGenerator.CA_CERT_CONFIGMAP + "/" + OcpMongoCertGenerator.CA_CERT_SUBPATH));
    }
}
