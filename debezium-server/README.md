# Debezium Server

Debezium Server is a standalone Java application built on Qurkus framework.
The application itself contains the `core` module and a set of modules responsible for communication with different target systems.

The per-module integration tests depend on the availability of the external services.
It is thus recommended to execute integration tests per-module and set-up necessary pre-requisities beforehand.

## Amazon Kinesis

* Execute `aws configure` as described in AWS CLI [getting started](https://github.com/aws/aws-cli#getting-started) guide and setup the account.
* Create Kinesis stream `aws kinesis create-stream --stream-name testc.inventory.customers --shard-count 1`
* Build the module and execute the tests `mvn clean install -DskipITs=false -am -pl debezium-server-kinesis`
* Remove the stream `aws kinesis delete-stream --stream-name testc.inventory.customers`

## Google Cloud Pub/Sub

* Login into your Google Cloud account using `gcloud auth application-default login` as described in the [documentation](https://cloud.google.com/sdk/gcloud/reference/auth/application-default).
* Create a new topic `gcloud pubsub topics create testc.inventory.customers`
* Build the module and execute the tests `mvn clean install -DskipITs=false -am -pl debezium-server-pubsub`
* Remove the topic `gcloud pubsub topics delete testc.inventory.customers`
