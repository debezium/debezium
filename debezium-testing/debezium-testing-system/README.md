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
ORACLE_ARTIFACT_DIR="${PWD}/oracle-libs/${ORACLE_ARTIFACT_VERSION}.0"

mkdir -p ${ORACLE_ARTIFACT_DIR}
cd ${ORACLE_ARTIFACT_DIR}
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=ojdbc8 \
    -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=ojdbc8.jar \
    -Dmaven.repo.local=${MAVEN_REPO}
mvn install:install-file -DgroupId=com.oracle.instantclient -DartifactId=xstreams \
    -Dversion=${ORACLE_ARTIFACT_VERSION} -Dpackaging=jar -Dfile=xstreams.jar \
    -Dmaven.repo.local=${MAVEN_REPO}
```
Build Debezium, build the artifact server and push it to image registry
``` bash
cd ${DEBEZIUM_DIR}
mvn clean install -DskipTests -DskipITs -Passembly -Dmaven.repo.local=local-maven-repo
```

For oracle
``` bash
mvn clean install -Passembly,oracle-all -DskipTests -DskipITs -Dmaven.repo.local=local-maven-repo
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
## Running the tests
```bash
mvn install -Docp.url=<ocp_api_url> \
  -Docp.username=<ocp_password> \
  -Docp.password=<ocp_password> \
  -Dimage.as=<artifact_server_image> \
  -Dgroups=!docker
```

The following properties can be set to further configure the test execution

| Name | Default Value | description                                       |
| -----| ------------- |---------------------------------------------------|
| ocp.url | | OpenShift API endpoint                            |
| ocp.username | | OpenShift admin username                          |
| ocp.password | | OpenShift admin password                          |
| ocp.project.debezium | debezium | OpenShift debezium project                        |
| ocp.project.mysql    | debezium-mysql | OpenShift mysql project                           |
| image.as             | | Artifact server image URL                         |
| ocp.pull.secret.paths | | Pull secret if artifact server is in private repo |

