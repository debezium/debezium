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

## Azure Event Hubs

### Create an Event Hubs namespace

Create an [Event Hubs namespace](https://docs.microsoft.com/azure/event-hubs/event-hubs-features?WT.mc_id=debezium-docs-abhishgu#namespace). Check the documentation for options on how do this using the [Azure Portal](https://docs.microsoft.com/azure/event-hubs/event-hubs-create?WT.mc_id=debezium-docs-abhishgu#create-an-event-hubs-namespace), [Azure CLI](https://docs.microsoft.com/azure/event-hubs/event-hubs-quickstart-cli?WT.mc_id=debezium-docs-abhishgu#create-an-event-hubs-namespace) etc.

### Create an Event Hub

Create an Event Hub (equivalent to a topic). Check the documentation for options on how do this using the [Azure Portal](https://docs.microsoft.com/azure/event-hubs/event-hubs-create?WT.mc_id=debezium-docs-abhishgu#create-an-event-hub), [Azure CLI](https://docs.microsoft.com/azure/event-hubs/event-hubs-quickstart-cli?WT.mc_id=debezium-docs-abhishgu#create-an-event-hub) etc.

### Build the module

[Get the Connection string](https://docs.microsoft.com/azure/event-hubs/event-hubs-get-connection-string?WT.mc_id=debezium-docs-abhishgu) required to communicate with Event Hubs. The format is: `Endpoint=sb://<NAMESPACE>/;SharedAccessKeyName=<ACCESS_KEY_NAME>;SharedAccessKey=<ACCESS_KEY_VALUE>`

Set environment variables required for tests:

```shell
export EVENTHUBS_CONNECTION_STRING=<Event Hubs connection string>
export EVENTHUBS_NAME=<name of the Event hub created in previous step>
```

Execute the tests:

```shell
mvn clean install -DskipITs=false -Deventhubs.connection.string=$EVENTHUBS_CONNECTION_STRING -Deventhubs.hub.name=$EVENTHUBS_NAME -am -pl debezium-server-eventhubs
```

### Clean up

Delete the Event Hubs namespace