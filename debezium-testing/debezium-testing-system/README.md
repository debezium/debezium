# OpenShift deployment verification suite
This project verifies the basic functionality of Debezium connectors with Kafka cluster deployed to OpenShift via Strimzi project.

## Prerequisites
OpenShift cluster with cluster-wide administrator access is required in order to run these tests.
Depending on chosen registry a configured docker credentials are required in order to push built

The tests also need an image containing debezium connector and scripting artifacts.
### Create artifact server
Create maven repo folder
``` bash
MAVEN_REPO=${PWD}/local-maven-repo
```

Prepare Apicurio converter
``` bash
APICURIO_VERSION=<apicurio_version>
mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get \
    -Dartifact=io.apicurio:apicurio-registry-distro-connect-converter:${APICURIO_VERSION}:zip \
    -Dmaven.repo.local=${MAVEN_REPO}
```

Add Oracle drivers for oracle connector
``` bash
ORACLE_ARTIFACT_VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=version.oracle.driver)
ORACLE_INSTANTCLIENT_ARTIFACT_VERSION=$(mvn -q -DforceStdout help:evaluate -Dexpression=version.oracle.instantclient)
ORACLE_ARTIFACT_DIR="${PWD}/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"

mkdir -p ${ORACLE_ARTIFACT_DIR}
cd ${ORACLE_ARTIFACT_DIR}
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 \
    -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar \
    -Dmaven.repo.local=${MAVEN_REPO}
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams \
    -Dversion=${ORACLE_INSTANTCLIENT_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar \
    -Dmaven.repo.local=${MAVEN_REPO}
```

Build Artifact server image and push it to image registry
``` bash
cd ${DEBEZIUM_DIR}/jenkins-jobs/scripts
./upstream-artifact-server-prepare.sh \
    --maven-repo ${MAVEN_REPO} \
    -r <image_registry> \
    -o <organization> \
    -t <image_tag> \
    --dest-login <username> \
    --dest-pass <password> \
    --oracle-included true \
    -d ${DEBEZIUM_DIR}
```

Build and push testsuite docker image (x86 platform)
```bash
    pushd debezium-testing/debezium-testing-system/src/test/resources/docker/
    docker build -t <your_image_repository> --no-cache .
    docker push <your_image_repository>
```

Prepare namespace and pull secret for the testsuite
```bash
    oc login -u <username> -p <password> <ocp-api>
    oc new-project debezium-testsuite
    
    oc create secret generic <pull_secret_name> \
    --from-file=.dockercfg=<path/to/.dockercfg> \
    --type=kubernetes.io/dockercfg \
    -n debezium-testsuite
```

Edit Pod template
- Update pod template `debezium-testing/debezium-testing-system/src/test/resources/kube/Pod.yaml` with custom values
  - If your testsuite image is private you have to link pull secret with default Service Account for pulling
- In case you want attach remote debugger add `DEBUG_PORT` environment variable to Pod template
- All the Pod template variables are described in table below

| Name                     | Description                                    | Example                                                                                             |
|--------------------------|------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| TESTSUITE_ARGUMENTS      | Arguments are passed to mvn command            | -Dtest.prepare.strimzi=true -DskipTests=true -Dtest.strimzi.version.kafka=3.5.0 -Dtest.wait.scale=1 |
| DBZ_GIT_BRANCH           | Branch from which you want start the tests     | "main"                                                                                              |
| DBZ_GIT_REPOSITORY       | Repository from which you want start the tests | "https://github.com/debezium/debezium.git"                                                          |
| DBZ_OCP_PROJECT_DEBEZIUM | Project where Kafka will be deployed           | "debezium"                                                                                          |
| DBZ_SECRET_NAME          | Pull secret for Artifact server repository     | "docker-secret"                                                                                     |
| DEBUG_PORT               | Port for remote debugging                      | "9000"                                                                                              |

More testsuite arguments

The following properties can be set to further configure the test execution (rest can be found in pom.xml, mostly with prefix test)

| Name | Default Value | description                                       |
| -----| ------------- |---------------------------------------------------|
| ocp.url | | OpenShift API endpoint                            |
| ocp.username | | OpenShift admin username                          |
| ocp.password | | OpenShift admin password                          |
| ocp.project.debezium | debezium | OpenShift debezium project                        |
| ocp.project.mysql    | debezium-mysql | OpenShift mysql project                           |
| image.as             | | Artifact server image URL                         |
| ocp.pull.secret.paths | | Pull secret if artifact server is in private repo |


## Running the tests
Deploy testsuite Pod and PVC
```bash
    oc apply -f debezium-testing/debezium-testing-system/src/test/resources/kube/PersistentVolumeClaim.yaml
    oc apply -f debezium-testing/debezium-testing-system/src/test/resources/kube/Pod.yaml
    oc adm policy add-cluster-role-to-user cluster-admin system:serviceaccount:debezium-testsuite:default
```
On some environments you can have problem creating PVC. In that case you use workaround before running Pod:
```bash
    oc adm policy add-scc-to-user anyuid -z default
```

### Debugging 
By default you cannot access pods running in Openshift by any other port than HTTPS.
You can either create load balancer service if your infrastructure allows it or you
can do `ocp port forward pod/testsuite <DEBUG_PORT> <DEBUG_PORT>` in separate terminal.
That will allow your connection to testsuite running remotely in the pod.




