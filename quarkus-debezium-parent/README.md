# Debezium Extensions for Quarkus

The following documentation explores how to **develop** a Quarkus extension that integrates Debezium.
If you need some guidelines on how to **use** Quarkus extension for Debezium, please use the [official documentation](https://debezium.io/documentation/reference/stable/integrations/quarkus-debezium-engine-extension.html).


## Before starting: The extension project

Before starting to create an extension, please take a look at the official documentation for [Quarkus extensions](https://quarkus.io/guides/writing-extensions).
Your extension project should be set up as a multi-module project with three submodules:

- runtime
- deployment
- integration-tests

### Principles to follow

Every extension should provide:

- Native build
- developer experience that follows the interfaces provided by the `quarkus-debezium-engine`
- Quarkus dev service that works with the connector

## Runtime module

The runtime module contains the extension behavior for the native executable or runtime JVM.
In the case of Debezium Extensions for Quarkus, the runtime contains the behavior necessary to correctly instrument the Debezium engine with configurations coming from Quarkus or other Extensions.

You should import at least the following dependencies:

```xml
        <dependency>
            <groupId>io.debezium.quarkus</groupId>
            <artifactId>quarkus-debezium-engine-spi</artifactId>
        </dependency>
        <dependency>
            <groupId>io.debezium.quarkus</groupId>
            <artifactId>quarkus-debezium-engine</artifactId>
        </dependency>
```

In the runtime module, you should implement the following interfaces:

- Configuration classes
  - `io.debezium.runtime.configuration.QuarkusDatasourceConfiguration` defines the Datasource configuration that can be taken from DevServices or Quarkus
  - `io.debezium.runtime.recorder.DatasourceRecorder` defines a [quarkus recorder](https://quarkus.io/guides/writing-extensions#bytecode-recording) that converts a reference to a connection to a `io.debezium.runtime.configuration.QuarkusDatasourceConfiguration`
- Connector producer
  - `io.debezium.runtime.ConnectorProducer` defines how to generate a `io.debezium.runtime.DebeziumConnectorRegistry` that contains information and the relative engines

In general, the code that is present in the runtime module should define how to map the different configurations with the embedded engine.


## Deployment module

The deployment module handles the build-time processing and bytecode recording.
In the case of Debezium Extensions for Quarkus, the deployment module instruments the extension with:

- extension information
- how to build the connector with the engine in native mode
- how to instantiate the dev services if needed

You should import the runtime module with at least the following dependencies:

```xml
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-core-deployment</artifactId>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-arc-deployment</artifactId>
        </dependency>
        <dependency>
            <groupId>io.debezium.quarkus</groupId>
            <artifactId>quarkus-debezium-engine-deployment</artifactId>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-jackson-deployment</artifactId>
        </dependency>
```

In the deployment module, you should implement the following interface:

- `io.quarkus.debezium.deployment.QuarkusEngineProcessor` guides the build-time processing with the necessary steps for a working extension

Even if it isn't strictly necessary to implement the `QuarkusEngineProcessor`, it helps to adhere to the expected beans that the `quarkus-debezium-engine` needs to work.


### Testing the deployment module

To test that the deployment module (and the runtime) are correct, you can use the `quarkus-debezium-testsuite` that provides a suite of tests that verify the correctness of the modules.
You should import the following dependency with test scope:

```xml
        <dependency>
            <groupId>io.debezium.quarkus</groupId>
            <artifactId>quarkus-debezium-testsuite-deployment</artifactId>
            <type>test-jar</type>
            <scope>test</scope>
        </dependency>
```

and implement the following interface:

- `QuarkusDebeziumNoSqlExtensionTestSuite` which contains tests for NoSQL datasources
or
- `QuarkusDebeziumSqlExtensionTestSuite` which contains tests for SQL datasources

In the class, you have to define which resources (like test containers) should start or stop when the test suite is running.
Please follow the guidelines present in the test suite [README.md](quarkus-debezium-testsuite-parent/README.md).
Furthermore, you have to create a `quarkus-debezium-testsuite.properties` file.

## Integration Tests module

The integration tests module is necessary to test the extension inside a Quarkus application in the JVM runtime or as a native executable.
To achieve this, the Quarkus Debezium test suite contains an integration testing suite that verifies that the extension works in both single-engine and multi-engine configurations.
As with the deployment testing, please follow the guidelines present in the test suite [README.md](quarkus-debezium-testsuite-parent/README.md).