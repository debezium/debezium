FROM registry.access.redhat.com/ubi8/openjdk-11
USER root
RUN microdnf install git   
RUN microdnf install unzip

ARG OCP_CLIENT=3.7.23

RUN curl --retry 7 -Lo /tmp/client-tools.tar.gz "https://mirror.openshift.com/pub/openshift-v3/clients/${OCP_CLIENT}/linux/oc.tar.gz"

RUN tar zxf /tmp/client-tools.tar.gz -C /usr/local/bin oc \
    && rm /tmp/client-tools.tar.gz

RUN microdnf clean all 
COPY testsuite-deployment.sh /testsuite/testsuite-deployment.sh
COPY library.sh /testsuite/library.sh
RUN chmod a+x /testsuite/testsuite-deployment.sh

ENTRYPOINT /testsuite/testsuite-deployment.sh 
