# OpenShift deployment verification suite
This project verifies the basic functionality of Debezium connectors with Kafka cluster deployed to OpenShift via Strimzi project.

## Prerequisites
OpenShift cluster with cluster-wide administrator access is required in order to run these tests.
Depending on chosen registry a configured docker credentials are required in order to push built

## Running the tests
```bash
mvn install -Docp.url=<ocp_api_url> -Docp.username=<ocp_password> -Docp.password=<ocp_password> -Dimage.fullname=<connect_image_name>
``` 

The following properties can be set to further configure the test execution

| Name | Default Value | description |
| -----| ------------- | ----------- |
| ocp.url | | OpenShift API endpoint |
| ocp.username | | OpenShift admin username |
| ocp.password | | OpenShift admin password |
| ocp.project.debezium | debezium | OpenShift debezium project |
| ocp.project.mysql | debezium-mysql | OpenShift mysql project |
| image.fullname | | Full name of Kafka Connect image |

## Building a KafkaConnect image with Debezium connectors

To build connect image running the ```assembly``` profile from parent directory together with ```image``` profile

```bash 
mvn clean install -DskipTests -DskipITs -Passembly -Pimage
```

The following properties can be set to further configure image build 

| Name | Default Value | description |
| -----| ------------- | ----------- |
| image.push.skip | true | Skips push to remote registry |
| image.push.registry | quay.io | remote registry base |
| image.name | quay.io/debezium/kafka:${project.version}-${image.version.strimzi}-kafka-${version.kafka} | Name of built image |
| image.fullname | ${image.push.registry}/${image.name} | Full name of the built image |
| image.base.name | strimzi/kafka:${image.version.strimzi}-kafka-${version.kafka} | Base for built image |
| image.version.strimzi | latest | Version of Strimzi Kafka image | 
