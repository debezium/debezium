# Quarkus Debezium TestSuite

This module provides a standardized test suite framework for Debezium Extensions for Quarkus, ensuring consistent test coverage across all connector implementations.

## Overview

The Debezium test suite is designed to ensure consistent and reliable validation of connector functionality across all implementations. 
It uses [junit-platform-suite](https://docs.junit.org/current/user-guide/#junit-platform-suite-engine) to specify set of tests to execute and resource setup and teardown logic. 
Every quarkus extensions is evaluated against the same baseline of functional and integration requirements.
The suite covers two main categories of tests. Deployment tests verify core behaviors such as:

- CapturingTest: Basic change data capture functionality
- DebeziumLifeCycleTest: Connector lifecycle management
- HeartbeatTest: Heartbeat mechanism verification
- NotificationTest: Event notification system
- PostProcessingTest: Data transformation and processing

Integration tests validate end-to-end scenarios in a quarkus sample app, including:

- CapturingIT: End-to-end change capture scenarios
- EngineIT: Embedded engine functionality
- NotificationIT: Event notification integration
- HeartbeatEventIT: Heartbeat event processing
- LifecycleEventIT: Lifecycle event management

By standardizing these tests, the suite ensures that all connectors meet the same functional expectations and behave consistently under common scenarios.

### Understanding the Test Scenarios

The testsuite expects a standardized data structure:

- **Database**: `inventory`
- **Collections** / **Table**: `orders`, `users`, `products`
- **Users Data**:
  ```
  | id | name     | description |
  | 1  | giovanni | developer   |
  | 2  | mario    | developer   |
  ```
- **Orders Data**:
  ```
  | key | name |
  | 1   | one  |
  | 2   | two  |
  ```

### Module Structure

```text
 quarkus-debezium-testsuite-parent/
 ├── deployment/                    # tests for deployment/runtime-time processing
 │   └── suite/                    # Standard test classes for deployment testing
 │       ├── CapturingTest.java    # Tests basic change capture functionality
 │       ├── DebeziumLifeCycleTest.java # Tests connector lifecycle
 │       ├── HeartbeatTest.java    # Tests heartbeat functionality
 │       ├── NotificationTest.java # Tests event notifications
 │       └── PostProcessingTest.java # Tests data transformations
 └── integration-tests/            # Runtime integration test framework
     ├── general/                  # Standard integration test classes
     │   ├── CapturingIT.java     # Integration capturing tests
     │   ├── EngineIT.java        # Engine integration tests
     │   └── NotificationIT.java  # Notification integration tests
     └── events/                   # Event-specific integration tests
         ├── HeartbeatEventIT.java
         └── LifecycleEventIT.java
```

## Integration Guide

### 1. Add TestSuite Dependencies

#### For Deployment Tests
Add to your connector's `deployment/pom.xml`:

```xml
<dependency>
    <groupId>io.debezium.quarkus</groupId>
    <artifactId>quarkus-debezium-testsuite-deployment</artifactId>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.junit.platform</groupId>
    <artifactId>junit-platform-suite</artifactId>
    <scope>test</scope>
</dependency>
```

#### For Integration Tests
Add to your connector's `integration-tests/pom.xml`:

```xml
<dependency>
    <groupId>io.debezium.quarkus</groupId>
    <artifactId>quarkus-debezium-testsuite-integration-tests</artifactId>
</dependency>
<dependency>
    <groupId>io.debezium.quarkus</groupId>
    <artifactId>quarkus-debezium-testsuite-integration-tests</artifactId>
    <type>test-jar</type>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.junit.platform</groupId>
    <artifactId>junit-platform-suite</artifactId>
    <scope>test</scope>
</dependency>
```

### 2. Add Test Suite in the codebase

#### For Deployment Tests

```java
@SuiteDisplayName("Your Connector Debezium Extensions Test Suite")
public class YourConnectorDeploymentTest implements QuarkusDebeziumNoSqlExtensionTestSuite {

    private static final YourConnectorTestResource testResource = new YourConnectorTestResource();

    @BeforeSuite
    public static void init() {
        // Setup connector-specific test data and resources
        testResource.start(
            // Your test data setup
        );
    }

    @AfterSuite
    public static void close() {
        testResource.stop();
    }
}
```

#### For Integration Tests

```java
public class YourConnectorIntegrationTestSuiteIT implements QuarkusDebeziumIntegrationTestSuite {
    // Empty implementation - automatically inherits all test classes from interface!
    // Container setup handled via Docker Maven Plugin in pom.xml
}
```

### 3. Configure Container Dependencies

#### Database Container Setup

Each connector requires its specific database container. Configure the Docker Maven Plugin in your `integration-tests/pom.xml`:

**MongoDB Example:**
```xml
<plugin>
    <groupId>io.fabric8</groupId>
    <artifactId>docker-maven-plugin</artifactId>
    <executions>
        <execution>
            <id>start-mongodb</id>
            <phase>pre-integration-test</phase>
            <goals><goal>start</goal></goals>
        </execution>
        <execution>
            <id>stop-mongodb</id>
            <phase>post-integration-test</phase>
            <goals><goal>stop</goal></goals>
        </execution>
    </executions>
    <configuration>
        <images>
            <image>
                <name>quay.io/debezium/mongo:6.0</name>
                <alias>mongodb</alias>
                <run>
                    <ports>
                        <port>27017:27017</port>
                    </ports>
                    <env>
                        <MONGO_INITDB_ROOT_USERNAME>native</MONGO_INITDB_ROOT_USERNAME>
                        <MONGO_INITDB_ROOT_PASSWORD>native</MONGO_INITDB_ROOT_PASSWORD>
                        <MONGO_INITDB_DATABASE>test</MONGO_INITDB_DATABASE>
                    </env>
                </run>
            </image>
        </images>
    </configuration>
</plugin>
```

### 4. Create Test Configuration

#### For Deployment Tests

Create `quarkus-debezium-testsuite.properties` in your test resources directory:

```properties
# Connector-specific configuration
quarkus.debezium.offset.storage=org.apache.kafka.connect.storage.MemoryOffsetBackingStore
quarkus.debezium.name=test
quarkus.debezium.topic.prefix=topic

# Test scenario expectations (as documented in test suite)
quarkus.debezium.database.include.list=inventory
quarkus.debezium.snapshot.mode=initial
quarkus.debezium.capturing.orders.destination=topic.inventory.orders

# Dev services should be disabled
quarkus.mongodb.devservices.enabled=false


# Heartbeat configuration (5ms as expected by tests)
quarkus.debezium.mongodb.heartbeat.interval.ms=5
```

#### For Integration Tests

Create an `application.properties` in your production resource directory.