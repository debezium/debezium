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
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStrategyBuilder;

public class OcpConfigServerModelProvider {

    public static String getConfigServerName(int num) {
        return OcpMongoShardedConstants.MONGO_CONFIG_DEPLOYMENT_NAME + num;
    }

    public static Deployment configServerDeployment(int num, String project) {
        String name = getConfigServerName(num);
        ObjectMeta metaData = new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(project)
                .withLabels(Map.of("app", "mongo",
                        "deployment", name,
                        "role", OcpMongoShardedConstants.MONGO_CONFIG_ROLE))
                .build();
        return new DeploymentBuilder()
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
                                                .withEmptyDir(new EmptyDirVolumeSource())
                                                .withName("volume-" + name)
                                                .build())
                                        .withContainers(new ContainerBuilder()
                                                .withName("mongo")
                                                .withVolumeMounts(new VolumeMountBuilder()
                                                        .withName("volume-" + name)
                                                        .withMountPath("/data/db")
                                                        .build())
                                                .withReadinessProbe(new ProbeBuilder()
                                                        .withExec(new ExecActionBuilder()
                                                                .withCommand("mongosh", "localhost:" + OcpMongoShardedConstants.MONGO_CONFIG_PORT)
                                                                .build())
                                                        .withInitialDelaySeconds(5)
                                                        .build())
                                                .withPorts(new ContainerPortBuilder()
                                                        .withProtocol("TCP")
                                                        .withContainerPort(OcpMongoShardedConstants.MONGO_CONFIG_PORT)
                                                        .build())
                                                .withImagePullPolicy("Always")
                                                .withTerminationMessagePolicy("File")
                                                .withTerminationMessagePath("/dev/termination-log")
                                                .withImage(ConfigProperties.DOCKER_IMAGE_MONGO_SHARDED)
                                                .withCommand("mongod",
                                                        "--configsvr",
                                                        "--replSet",
                                                        OcpMongoShardedConstants.MONGO_CONFIG_REPLICASET_NAME,
                                                        "--dbpath",
                                                        "/data/db",
                                                        "--bind_ip_all")
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    public static Service configServerService(int num, String project) {
        String name = getConfigServerName(num);
        return new ServiceBuilder()
                .withKind("Service")
                .withApiVersion("v1")
                .withMetadata(new ObjectMetaBuilder()
                        .withName(name)
                        .withNamespace(project)
                        .build())
                .withSpec(new ServiceSpecBuilder()
                        .withSelector(Map.of("app", "mongo",
                                "deployment", name,
                                "role", OcpMongoShardedConstants.MONGO_CONFIG_ROLE))
                        .withPorts(new ServicePortBuilder()
                                .withName("db")
                                .withPort(OcpMongoShardedConstants.MONGO_CONFIG_PORT)
                                .withTargetPort(new IntOrString(OcpMongoShardedConstants.MONGO_CONFIG_PORT))
                                .build())
                        .build())
                .build();
    }

}
