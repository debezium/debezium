/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.builders;

import java.util.Map;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.OcpMongoShardedConstants;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import io.fabric8.kubernetes.api.model.TCPSocketActionBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;

public class OcpShardModelFactory {
    public static final String ROLE = "shard";

    public static Deployment shardDeployment(int shardNum, int replicaNum) {
        String name = getShardName(shardNum, replicaNum);
        ObjectMeta metaData = new ObjectMetaBuilder()
                .withName(name)
                .withLabels(Map.of("app", "mongo",
                        "deployment", name,
                        "role", ROLE))
                .build();
        DeploymentBuilder builder = new DeploymentBuilder()
                .withKind("Deployment")
                .withApiVersion("apps/v1")
                .withMetadata(metaData)
                .withSpec(new DeploymentSpecBuilder()
                        .withReplicas(1)
                        .withStrategy(new DeploymentStrategyBuilder()
                                .withType("Recreate")
                                .build())
                        .withSelector(new LabelSelectorBuilder()
                                .withMatchLabels(metaData.getLabels())
                                .build())
                        .withTemplate(new PodTemplateSpecBuilder()
                                .withMetadata(new ObjectMetaBuilder()
                                        .withLabels(metaData.getLabels())
                                        .build())
                                .withSpec(new PodSpecBuilder()
                                        .withVolumes(new VolumeBuilder()
                                                .withName("volume-" + name)
                                                .withEmptyDir(new EmptyDirVolumeSource())
                                                .build())
                                        .withContainers(new ContainerBuilder()
                                                .withName("mongo")
                                                .withPorts(new ContainerPortBuilder()
                                                        .withProtocol("TCP")
                                                        .withContainerPort(OcpMongoShardedConstants.MONGO_SHARD_PORT)
                                                        .build())
                                                .withImagePullPolicy("Always")
                                                .withVolumeMounts(new VolumeMountBuilder()
                                                        .withName("volume-" + name)
                                                        .withMountPath("/data/db")
                                                        .build())
                                                .withLivenessProbe(new ProbeBuilder()
                                                        .withInitialDelaySeconds(10)
                                                        .withTcpSocket(new TCPSocketActionBuilder()
                                                                .withPort(new IntOrString(OcpMongoShardedConstants.MONGO_SHARD_PORT))
                                                                .build())
                                                        .withTimeoutSeconds(20)
                                                        .build())
                                                .withTerminationMessagePolicy("File")
                                                .withTerminationMessagePath("/dev/termination-log")
                                                .withImage(ConfigProperties.DOCKER_IMAGE_MONGO_SHARDED)
                                                .withCommand("mongod",
                                                        "--shardsvr",
                                                        "--replSet",
                                                        "shard" + shardNum + "rs",
                                                        "--dbpath",
                                                        "/data/db",
                                                        "--bind_ip_all")
                                                .build())
                                        .build())
                                .build())
                        .build());
        return builder.build();
    }

    public static Service shardService(int shardNum, int replicaNum) {
        String name = getShardName(shardNum, replicaNum);
        return new ServiceBuilder()
                .withKind("Service")
                .withApiVersion("v1")
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .build())
                .withSpec(new ServiceSpecBuilder()
                        .withSelector(Map.of("app", "mongo",
                                "deployment", name,
                                "role", "shard"))
                        .withPorts(new ServicePortBuilder()
                                .withName("db")
                                .withPort(27018)
                                .withTargetPort(new IntOrString(27018))
                                .build())
                        .build())
                .build();
    }

    private static String getShardName(int shardNum, int replicaNum) {
        return OcpMongoShardedConstants.MONGO_SHARD_DEPLOYMENT_PREFIX + shardNum + "r" + replicaNum;
    }
}
