# Debezium Extensions for Quarkus

The following documentation explore how to **develop** a quarkus extension that integrate Debezium.
If you need some guideline in how to **use** quarkus extension for Debezium, please use the [official documentation](https://debezium.io/documentation/reference/stable/integrations/quarkus-debezium-engine-extension.html).


## Before start: The extension project

Before start creating an extension, please take a look to the official documentation for [Quarkus extensions](https://quarkus.io/guides/writing-extensions).
Your extension project should be setup as a multi-module project with three submodules:

- runtime
- deployment
- integration-tests

### Principles to follow

Every extension should provide:

- Native build
- developer experience that follows the interfaces provided by the `quarkus-debezium-engine`
- Quarkus dev service that works with the connector

## Runtime module

The runtime module contains the extension behaviour for the native executable or runtime JVM.
In the case of Debezium Extensions for Quarkus, in the runtime is present the behaviour necessary to instrument correctly the debezium engine with configurations coming from Quarkus or other Extensions.

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

In general the code that is present in the runtime module, should define how to map the different configurations with the embedded engine.


## Deployment module

The deployment module handles the build time processing and bytecode recording.
In the case of Debezium Extensions for Quarkus, instrument the extension:

- extensions information
- how to build the connector with the engine in native mode
- how to instantiate eventually the dev services

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

- `io.quarkus.debezium.deployment.QuarkusEngineProcessor` guides the build time processing with the necessary steps for a working extensions

Even if it isn't strictly necessary implement the `QuarkusEngineProcessor`, it helps to adhere to the expected beans that the `quarkus-debezium-engine` needs to work.


### Testing the deployment module

To test that the deployment module (and the runtime) are correct, you can use the `quarkus-debezium-testsuite` that provides a suite of tests that control the correctness of the modules.
You should import the following dependency as test scope:

```xml
        <dependency>
            <groupId>io.debezium.quarkus</groupId>
            <artifactId>quarkus-debezium-testsuite-deployment</artifactId>
            <type>test-jar</type>
            <scope>test</scope>
        </dependency>
```

and implement the following interface:

- `QuarkusDebeziumNoSqlExtensionTestSuite` which contains tests for no-sql datasource
or
- `QuarkusDebeziumSqlExtensionTestSuite` which contains tests for sql datasource

in the class you have to define which resource (like a test container) should start or stop when the test suite is running.
Please follow the guidelines present in the test suite [README.md](quarkus-debezium-testsuite-parent/README.md).
Furthermore, you have to create a `quarkus-debezium-testsuite.properties`

## Integration Tests module

The integration tests module is necessary to test the extension inside a Quarkus application in the JVM runtime or as Native executable.
To achieve this, the quarkus debezium test suite contains an integration testing suite that controls that the extension works in a single or multiengine manner.
Please as for the deployment testing, follow the guidelines present in the test suite [README.md](quarkus-debezium-testsuite-parent/README.md).