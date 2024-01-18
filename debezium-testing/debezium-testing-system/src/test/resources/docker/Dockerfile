FROM fedora:35 AS base

RUN dnf -y install git unzip zip findutils rsync

RUN curl --retry 7 -Lo /tmp/client-tools.tar.gz "https://mirror.openshift.com/pub/openshift-v4/amd64/clients/ocp/latest/openshift-client-linux.tar.gz"
RUN tar zxf /tmp/client-tools.tar.gz -C /usr/local/bin oc \
    && rm /tmp/client-tools.tar.gz

ARG sdkman_init="/root/.sdkman/bin/sdkman-init.sh"

RUN curl -s "https://get.sdkman.io" | bash
RUN source $sdkman_init &&\
    sdk install java 17.0.2-open &&\
    sdk install maven

RUN mkdir /root/testsuite
COPY testsuite.sh /root/testsuite/testsuite.sh
RUN chmod a+x /root/testsuite/testsuite.sh

WORKDIR /root/testsuite

ENTRYPOINT /root/testsuite/testsuite.sh
