FROM fedora:35 AS base

RUN dnf -y install git unzip zip findutils

ARG OCP_CLIENT=3.7.23
RUN curl --retry 7 -Lo /tmp/client-tools.tar.gz "https://mirror.openshift.com/pub/openshift-v3/clients/${OCP_CLIENT}/linux/oc.tar.gz"
RUN tar zxf /tmp/client-tools.tar.gz -C /usr/local/bin oc \
    && rm /tmp/client-tools.tar.gz

ARG sdkman_init="/root/.sdkman/bin/sdkman-init.sh"

RUN curl -s "https://get.sdkman.io" | bash
RUN source $sdkman_init &&\
    sdk install java 17.0.2-open

# this runs when only when dbz repo/branch is changed
ARG repository
ARG branch

RUN git clone --branch $branch $repository /testsuite/debezium
RUN source $sdkman_init && pushd /testsuite/debezium && ./mvnw clean install -DskipTests -DskipITs

COPY testsuite-deployment.sh /testsuite/testsuite-deployment.sh
RUN chmod a+x /testsuite/testsuite-deployment.sh

COPY library.sh /testsuite/library.sh

ENTRYPOINT /testsuite/testsuite-deployment.sh

FROM base AS downstream

COPY apicurio-registry-install-examples.zip /testsuite/apicurio.zip
COPY amq-streams-install-examples.zip /testsuite/strimzi.zip
