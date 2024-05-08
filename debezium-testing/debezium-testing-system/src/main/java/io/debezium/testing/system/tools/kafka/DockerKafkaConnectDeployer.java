/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.kafka;

import io.debezium.testing.system.tools.AbstractDockerDeployer;
import io.debezium.testing.system.tools.Deployer;
import io.debezium.testing.system.tools.kafka.docker.KafkaConnectConainer;

import okhttp3.OkHttpClient;

public class DockerKafkaConnectDeployer
        extends AbstractDockerDeployer<DockerKafkaConnectController, KafkaConnectConainer>
        implements Deployer<DockerKafkaConnectController> {

    private DockerKafkaConnectDeployer(KafkaConnectConainer container) {
        super(container);
    }

    @Override
    protected DockerKafkaConnectController getController(KafkaConnectConainer container) {
        return new DockerKafkaConnectController(container, new OkHttpClient());
    }

    public static class Builder
            extends AbstractDockerDeployer.DockerBuilder<Builder, KafkaConnectConainer, DockerKafkaConnectDeployer> {

        public Builder() {
            this(new KafkaConnectConainer());
        }

        public Builder(KafkaConnectConainer container) {
            super(container);
        }

        public Builder withKafka(DockerKafkaController kafka) {
            container.withKafka(kafka.getKafkaContainer());
            return self();
        }

        @Override
        public DockerKafkaConnectDeployer build() {
            return new DockerKafkaConnectDeployer(container);
        }
    }
}
