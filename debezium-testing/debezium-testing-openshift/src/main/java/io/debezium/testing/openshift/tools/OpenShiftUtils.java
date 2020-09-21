/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.openshift.tools;

import static io.debezium.testing.openshift.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyPort;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * Utility methods for working with OpenShift
 * @author Jakub Cechacek
 */
public class OpenShiftUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenShiftUtils.class);

    private OpenShiftClient client;

    public OpenShiftUtils(OpenShiftClient client) {
        this.client = client;
    }

    /**
     * Creates route in given namespace
     * @param project project where this route will be created
     * @param name name of the route
     * @param service target service
     * @param port target port
     * @param labels labels set to set on this route
     * @return {@link Route} object for created route
     */
    public Route createRoute(String project, String name, String service, String port, Map<String, String> labels) {
        Route route = client.routes().inNamespace(project).createOrReplaceWithNew()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withNewTo()
                .withKind("Service")
                .withName(service)
                .endTo()
                .withNewPort()
                .withNewTargetPort(port)
                .endPort()
                .endSpec()
                .done();
        return route;
    }

    public Service createService(String project, String name, String portName, int port, Map<String, String> selector, Map<String, String> labels) {
        Service service = client.services().inNamespace(project).createOrReplaceWithNew()
                .withNewMetadata()
                .withName(name)
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .addNewPort()
                .withProtocol("TCP")
                .withName(portName).withPort(port).withTargetPort(new IntOrString(port))
                .endPort()
                .withSelector(selector)
                .endSpec().done();
        return service;
    }

    /**
     * Creates new NetworkPolicy in given namespace allowing public access
     * @param project project where this network policy will be created
     * @param name name of the policy
     * @param podSelectorLabels labels used as pod selectors
     * @param ports ports for which access will be allowed
     * @return {@link NetworkPolicy} object for created policy
     */
    public NetworkPolicy createNetworkPolicy(String project, String name, Map<String, String> podSelectorLabels, List<NetworkPolicyPort> ports) {
        NetworkPolicy policy = client.network().networkPolicies().inNamespace(project)
                .createOrReplaceWithNew()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .withNewSpec()
                .withNewPodSelector()
                .withMatchLabels(podSelectorLabels)
                .endPodSelector()
                .addNewIngress()
                .addToPorts(ports.toArray(new NetworkPolicyPort[ports.size()]))
                .endIngress()
                .withPolicyTypes("Ingress")
                .endSpec()
                .done();

        return policy;
    }

    /**
     * Links pull secret to service account
     * @param project project where this operation happens
     * @param sa service account name
     * @param secret secret name
     * @return {@link} Service account object to which this secret was linked
     */
    public ServiceAccount linkPullSecret(String project, String sa, String secret) {
        ServiceAccount serviceAccount = client.serviceAccounts().inNamespace(project).withName(sa).get();
        boolean linked = serviceAccount.getImagePullSecrets().stream().anyMatch(r -> r.getName().equals(secret));
        if (!linked) {
            return client.serviceAccounts().inNamespace(project).withName(sa).edit()
                    .addNewImagePullSecret().withName(secret).endImagePullSecret()
                    .addNewSecret().withName(secret).endSecret()
                    .done();
        }
        return serviceAccount;
    }

    /**
     * Ensures each container of given deployment has a environment variable
     * @param deployment deployment
     * @param envVar environment variable
     */
    public void ensureHasEnv(Deployment deployment, EnvVar envVar) {
        deployment.getSpec().getTemplate().getSpec().getContainers().forEach(c -> this.ensureHasEnv(c, envVar));
    }

    /**
     * Ensures container has a environment variable
     * @param container container
     * @param envVar environment variable
     */
    public void ensureHasEnv(Container container, EnvVar envVar) {
        List<EnvVar> env = container.getEnv();
        if (env == null) {
            env = new ArrayList<>();
            container.setEnv(env);
        }
        env.removeIf(var -> Objects.equals(var.getName(), envVar.getName()));
        env.add(envVar);
    }

    public void ensureHasPullSecret(Deployment deployment, String secret) {
        List<LocalObjectReference> secrets = deployment.getSpec().getTemplate().getSpec().getImagePullSecrets();
        if (secrets == null) {
            secrets = new ArrayList<>();
            deployment.getSpec().getTemplate().getSpec().setImagePullSecrets(secrets);
        }
        secrets.removeIf(s -> Objects.equals(secret, s.getName()));
        secrets.add(new LocalObjectReference(secret));
    }

    /**
     * Finds pods with given labels
     * @param project project where to look for pods
     * @param labels labels used to identify pods
     * @return {@link PodList} of matching pods
     */
    public PodList podsWithLabels(String project, Map<String, String> labels) {
        Supplier<PodList> podListSupplier = () -> client.pods().inNamespace(project).withLabels(labels).list();
        await().atMost(scaled(5), TimeUnit.MINUTES).until(() -> podListSupplier.get().getItems().size() > 0);
        PodList pods = podListSupplier.get();

        if (pods.getItems().isEmpty()) {
            LOGGER.warn("Empty PodList");
        }

        return pods;
    }

    /**
     * Waits until all pods with given labels are ready
     * @param project project where to look for pods
     * @param labels labels used to identify pods
     */
    public void waitForPods(String project, Map<String, String> labels) {
        String lbls = labels.keySet().stream().map(k -> k + "=" + labels.get(k)).collect(Collectors.joining(", "));
        LOGGER.info("Waiting for pods to deploy [" + lbls + "]");

        PodList pods = podsWithLabels(project, labels);

        for (Pod p : pods.getItems()) {
            try {
                client.resource(p).waitUntilReady(scaled(5), TimeUnit.MINUTES);
            }
            catch (InterruptedException e) {
                throw new IllegalStateException("Error when waiting for pod " + p.getMetadata().getName() + " to get ready", e);
            }
        }
    }
}
