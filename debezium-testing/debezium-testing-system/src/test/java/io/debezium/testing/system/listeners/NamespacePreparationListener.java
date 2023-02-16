/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.listeners;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.testing.system.tools.ConfigProperties;
import io.debezium.testing.system.tools.OpenShiftUtils;
import io.debezium.testing.system.tools.WaitConditions;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.openshift.api.model.ClusterRoleBindingBuilder;
import io.fabric8.openshift.api.model.Project;
import io.fabric8.openshift.api.model.ProjectBuilder;
import io.fabric8.openshift.client.OpenShiftClient;

public class NamespacePreparationListener implements TestExecutionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(NamespacePreparationListener.class);

    private OpenShiftClient client;
    private List<String> projectNames;

    public void testPlanExecutionStarted(TestPlan testPlan) {
        // execute only before integration tests
        if (ConfigProperties.OCP_PROJECT_DBZ != null) {
            projectNames = List.of(ConfigProperties.OCP_PROJECT_DBZ,
                    ConfigProperties.OCP_PROJECT_ORACLE,
                    ConfigProperties.OCP_PROJECT_MONGO,
                    ConfigProperties.OCP_PROJECT_DB2,
                    ConfigProperties.OCP_PROJECT_MYSQL,
                    ConfigProperties.OCP_PROJECT_POSTGRESQL,
                    ConfigProperties.OCP_PROJECT_REGISTRY,
                    ConfigProperties.OCP_PROJECT_SQLSERVER);
            client = OpenShiftUtils.createOcpClient();

            validateSystemParameters();
            prepareNamespaces();
        }
    }

    public void testPlanExecutionFinished(TestPlan testPlan) {
        // execute only after integration tests
        if (ConfigProperties.OCP_PROJECT_DBZ != null) {
            LOGGER.info("Cleaning namespaces");

            // delete projects if project names are set
            projectNames.forEach(name -> {
                Project project = client.projects().withName(name).get();
                if (project != null) {
                    client.projects().delete(project);
                }
            });

            client.close();
        }
    }

    private void prepareNamespaces() {
        LOGGER.info("Preparing namespaces");

        ClusterRoleBindingBuilder anyUidBindingBuilder = new ClusterRoleBindingBuilder()
                .withApiVersion("authorization.openshift.io/v1")
                .withKind("ClusterRoleBinding")
                .withMetadata(new ObjectMetaBuilder()
                        .withName("system:openshift:scc:anyuid")
                        .build())
                .withRoleRef(new ObjectReferenceBuilder()
                        .withName("system:openshift:scc:anyuid")
                        .build());

        ClusterRoleBindingBuilder privilegedBindingBuilder = new ClusterRoleBindingBuilder()
                .withApiVersion("authorization.openshift.io/v1")
                .withKind("ClusterRoleBinding")
                .withMetadata(new ObjectMetaBuilder()
                        .withName("system:openshift:scc:privileged")
                        .build())
                .withRoleRef(new ObjectReferenceBuilder()
                        .withName("system:openshift:scc:privileged")
                        .build());

        for (String project : projectNames) {
            processNamespace(project, anyUidBindingBuilder, privilegedBindingBuilder);
        }

        waitForDefaultServiceAccount();
        client.clusterRoleBindings().createOrReplace(anyUidBindingBuilder.build());
        client.clusterRoleBindings().createOrReplace(privilegedBindingBuilder.build());
    }

    private void waitForDefaultServiceAccount() {
        projectNames.forEach(name -> await().atMost(WaitConditions.scaled(1), TimeUnit.MINUTES)
                .pollInterval(1, SECONDS)
                .until(() -> client.serviceAccounts().inNamespace(name).withName("default").get() != null));
    }

    private void processNamespace(String namespace, ClusterRoleBindingBuilder anyuidBuilder, ClusterRoleBindingBuilder privilegedBuilder) {
        if (client.projects().withName(namespace).get() == null) {
            client.projects().createOrReplace(new ProjectBuilder()
                    .withKind("Project")
                    .withApiVersion("project.openshift.io/v1")
                    .withMetadata(new ObjectMetaBuilder()
                            .withName(namespace)
                            .build())
                    .build());
        }
        addServiceAccountToClusterRoleBinding(namespace, anyuidBuilder);
        addServiceAccountToClusterRoleBinding(namespace, privilegedBuilder);
    }

    private void addServiceAccountToClusterRoleBinding(String saNamespace, ClusterRoleBindingBuilder bindingBuilder) {
        bindingBuilder.addNewSubjectLike(new ObjectReferenceBuilder()
                .withKind("SystemUser")
                .withName("default")
                .withNamespace(saNamespace)
                .build());
        bindingBuilder.addNewUserName("system:serviceaccount:" + saNamespace + ":default");
    }

    /**
     * Check for invalid states of test parameters related to namespace preparation
     */
    private void validateSystemParameters() {
        if (ConfigProperties.PREPARE_NAMESPACES_AND_STRIMZI && namespacesExist()) {
            LOGGER.warn("Should prepare strimzi operator, but namespaces exist. Using existing namespaces");
        }

        if (!ConfigProperties.PREPARE_NAMESPACES_AND_STRIMZI && !ConfigProperties.PRODUCT_BUILD) {
            throw new IllegalStateException("PREPARE_STRIMZI is false in upstream build");
        }

        if (!ConfigProperties.PREPARE_NAMESPACES_AND_STRIMZI && !namespacesExist()) {
            throw new IllegalStateException("should not prepare strimzi/namespace but namespace is missing");
        }
    }

    private boolean namespacesExist() {
        assertThat(projectNames).isNotEmpty();
        assertThat(client).isNotNull();
        return client.projects().withName(projectNames.get(0)).get() != null;
    }
}
