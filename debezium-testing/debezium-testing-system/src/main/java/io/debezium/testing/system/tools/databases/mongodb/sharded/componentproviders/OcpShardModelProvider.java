/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb.sharded.componentproviders;

import java.util.Map;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.databases.mongodb.sharded.OcpMongoShardedConstants;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.ExecActionBuilder;
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

public class OcpShardModelProvider {

    public static Deployment shardDeployment(int shardNum, int replicaNum, String project) {
        String name = getShardNodeName(shardNum, replicaNum);
        ObjectMeta metaData = getMetaData(shardNum, replicaNum, project);
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
                                                .withVolumeMounts(new VolumeMountBuilder()
                                                        .withName("volume-" + name)
                                                        .withMountPath("/data/db")
                                                        .build())
                                                .withReadinessProbe(new ProbeBuilder()
                                                        .withExec(new ExecActionBuilder()
                                                                .withCommand("mongosh", "localhost:" + OcpMongoShardedConstants.MONGO_SHARD_PORT)
                                                                .build())
                                                        .withInitialDelaySeconds(5)
                                                        .build())
                                                .withPorts(new ContainerPortBuilder()
                                                        .withProtocol("TCP")
                                                        .withContainerPort(OcpMongoShardedConstants.MONGO_SHARD_PORT)
                                                        .build())
                                                .withImagePullPolicy("Always")
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
                                                        getShardReplicaSetName(shardNum),
                                                        "--dbpath",
                                                        "/data/db",
                                                        "--bind_ip_all")
                                                .build())
                                        .build())
                                .build())
                        .build());
        return builder.build();
    }

    public static Service shardService(int shardNum, int replicaNum, String project) {
        ObjectMeta metaData = getMetaData(shardNum, replicaNum, project);
        return new ServiceBuilder()
                .withKind("Service")
                .withApiVersion("v1")
                .withMetadata(metaData)
                .withSpec(new ServiceSpecBuilder()
                        .withSelector(metaData.getLabels())
                        .withPorts(new ServicePortBuilder()
                                .withName("db")
                                .withPort(OcpMongoShardedConstants.MONGO_SHARD_PORT)
                                .withTargetPort(new IntOrString(OcpMongoShardedConstants.MONGO_SHARD_PORT))
                                .build())
                        .build())
                .build();
    }

    public static String getShardNodeName(int shardNum, int replicaNum) {
        return OcpMongoShardedConstants.MONGO_SHARD_DEPLOYMENT_PREFIX + shardNum + "r" + replicaNum;
    }

    public static String getShardReplicaSetName(int shardNum) {
        return OcpMongoShardedConstants.MONGO_SHARD_DEPLOYMENT_PREFIX + shardNum + "rs";
    }

    private static ObjectMeta getMetaData(int shardNum, int replicaNum, String project) {
        String name = getShardNodeName(shardNum, replicaNum);
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(project)
                .withLabels(Map.of("app", "mongo",
                        "deployment", name,
                        "shard", String.valueOf(shardNum),
                        "role", OcpMongoShardedConstants.MONGO_SHARD_ROLE))
                .build();
    }
}
