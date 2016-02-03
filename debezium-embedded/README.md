# Embedding Debezium connectors in applications

Debezium connectors are normally operated by deploying them to a Kafka Connect service, and configuring one or more connectors to monitor upstream databases and produce data change events for all changes that they sees in the upstream databases. Those data change events are written to Kafka, where they can be independently consumed by many different applications. Kafka Connect provides excellent fault tolerance and scalability, since it runs as a distributed service and ensures that all registered and configured connectors are always running. For example, even if one of the Kafka Connect endpoints in a cluster goes down, the remaining Kafka Connect endpoints will restart any connectors that were previously running on the now-terminated endpoint.

Not every applications needs this level of fault tolerance and reliability, and they may not want to rely upon an external cluster of Kafka brokers and Kafka Connect services. Instead, some applications would prefer to *embed* Debezium connectors directly within the application space. They still want the same data change events, but prefer to have the connectors send them directly to the application rather than persiste them inside Kafka.

This `debezium-embedded` module defines a small library that allows an application to easily configure and run Debezium connectors.

## Dependencies

To use this module, add the `debezium-embedded` module to your application's dependencies. For Maven, this entails adding the following to your application's POM:

    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-embedded</artifactId>
        <version>${version.debezium}</version>
    </dependency>

where `${version.debezium}` is either the version of Debezium you're using or a Maven property whose value contains the Debezium version string.

Likewise, add dependencies for any of Debezium's connectors that your application will use. For example, the following can be added to your application's Maven POM file so your application can use the MySQL connector:

    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-connector-mysql</artifactId>
        <version>${version.debezium}</version>
    </dependency>

## In the code

Your application needs to set up an `EmbeddedConnector` for each connector instance you want to run. The `io.debezium.embedded.EmbeddedConnector` class serves as an easy-to-use wrapper around any Kafka Connect connector and completely manages the real connector's lifecycle. Basically, you create the `EmbeddedConnector` with a configuration that defines the environment for the `EmbeddedConnector` and the properties for the underlying connector. You also provide it with a function that the `EmbeddedConnector` will call whenever the underlying connector produces a data change event.

Let's see what this looks like when we use the MySQL connector:

    // Define the configuration for the embedded and MySQL connector ...
    Configuration config = Configuration.create()
                                        .with(EmbeddedConnector.CONNECTOR_NAME, "file-connector")
                                        .with(EmbeddedConnector.CONNECTOR_CLASS, "io.debezium.connector.mysql.MySqlConnector")
                                        .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                                        .with(MySqlConnectorConfig.PORT, 3306)
                                        .with(MySqlConnectorConfig.USER, "mysqluser")
                                        .with(MySqlConnectorConfig.PASSWORD, "mysqlpw")
                                        .with(MySqlConnectorConfig.SERVER_ID, 85744)
                                        .with(MySqlConnectorConfig.SERVER_NAME, "my-app-connector")
                                                         .build())
# Embedding Debezium connectors in applications

Debezium connectors are normally operated by deploying them to a Kafka Connect service, and configuring one or more connectors to monitor upstream databases and produce data change events for all changes that they sees in the upstream databases. Those data change events are written to Kafka, where they can be independently consumed by many different applications. Kafka Connect provides excellent fault tolerance and scalability, since it runs as a distributed service and ensures that all registered and configured connectors are always running. For example, even if one of the Kafka Connect endpoints in a cluster goes down, the remaining Kafka Connect endpoints will restart any connectors that were previously running on the now-terminated endpoint.

Not every applications needs this level of fault tolerance and reliability, and they may not want to rely upon an external cluster of Kafka brokers and Kafka Connect services. Instead, some applications would prefer to *embed* Debezium connectors directly within the application space. They still want the same data change events, but prefer to have the connectors send them directly to the application rather than persiste them inside Kafka.

This `debezium-embedded` module defines a small library that allows an application to easily configure and run Debezium connectors.

## Dependencies

To use this module, add the `debezium-embedded` module to your application's dependencies. For Maven, this entails adding the following to your application's POM:

    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-embedded</artifactId>
        <version>${version.debezium}</version>
    </dependency>

where `${version.debezium}` is either the version of Debezium you're using or a Maven property whose value contains the Debezium version string.

Likewise, add dependencies for any of Debezium's connectors that your application will use. For example, the following can be added to your application's Maven POM file so your application can use the MySQL connector:

    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-connector-mysql</artifactId>
        <version>${version.debezium}</version>
    </dependency>

## In the code

Your application needs to set up an `EmbeddedConnector` for each connector instance you want to run. The `io.debezium.embedded.EmbeddedConnector` class serves as an easy-to-use wrapper around any Kafka Connect connector and completely manages the real connector's lifecycle. Basically, you create the `EmbeddedConnector` with a configuration that defines the environment for the `EmbeddedConnector` and the properties for the underlying connector. You also provide it with a function that the `EmbeddedConnector` will call whenever the underlying connector produces a data change event.

Here's an example of code that configures and runs an embedded MySQL connector:

    // Define the configuration for the embedded and MySQL connector ...
    Configuration config = Configuration.create()
                                        /* begin embedded connector properties */
                                        .with("name", "my-sql-connector")
                                        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                                        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                                        .with("offset.storage.file.filename", "/path/to/storage/offset.dat")
                                        .with("offset.flush.interval.ms", 60000)
                                        /* begin wrapped connector properties */
                                        .with("database.hostname", "localhost")
                                        .with("database.port", 3306)
                                        .with("database.user", "mysqluser")
                                        .with("database.password", "mysqlpw")
                                        .with("server.id", 85744)
                                        .with("server.name", "my-app-connector")
                                        .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                                        .with("database.history.file.filename", "/path/to/storage/dbhistory.dat")
                                        .build())

    // Create the connector with this configuration ...
	EmbeddedConnector connector = EmbeddedConnector.create()
                                                   .using(config)
                                                   .notifying(this::handleEvent)
                                                   .build();
    
    // Run the connector asynchronously ...
    Executor executor = ...
    executor.execute(connector);

    // At some later time ...
    connector.stop();

Let's look into this code in more detail, starting with the first few lines that we repeat here:

   // Define the configuration for the embedded and MySQL connector ...
    Configuration config = Configuration.create()
                                        /* begin embedded connector properties */
                                        .with("name", "mysql-connector")
                                        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                                        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                                        .with("offset.storage.file.filename", "/path/to/storage/offset.dat")
                                        .with("offset.flush.interval.ms", 60000)

This creates a new `Configuration` object and, using a builder-style API, sets several fields required by the `EmbeddedConnector`. The first is a name for the connector that will be used within the source records produced by the connector, so use something meaningful in your application. The `connector.class` field defines the name of the class that implements the Kafka Connect `org.apache.kafka.connect.source.SourceConnector` abstract class; in this example, we specify Debezium's `MySqlConnector` class.

When a Kafka Connect connector runs, it reads information from the source and periodically records "offsets" that define how much of that information it has processed. Should the connector be restarted, it will use the last recorded offset to know where in the source information it should resume reading. The next few fields define that the embedded connector should use the `FileOffsetBackingStore` class to store offsets in the `/path/to/storage/offset.dat` file on the local file system (which can be named anything). Additionally, although the connector produces offsets with each source record it produces, offsets should be flushed to the store once every minute. These fields can be tailored as needed for your application.

The next few lines define the fields that are specific to the `MySqlConnector`:

                                        /* begin wrapped connector properties */
                                        .with("database.hostname", "localhost")
                                        .with("database.port", 3306)
                                        .with("database.user", "mysqluser")
                                        .with("database.password", "mysqlpw")
                                        .with("server.id", 85744)
                                        .with("server.name", "products")
                                        .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                                        .with("database.history.file.filename", "/path/to/storage/dbhistory.dat")
                                        .build())

Here, we set the name of the host machine and port number where the MySQL database server is running, and we define the username and password that will be used to connect to the MySQL database. The username and password should correspond to a MySQL database user that has been granted the [`REPLICATION SLAVE` privilege](http://dev.mysql.com/doc/refman/5.7/en/replication-howto-repuser.html).

The configuration also includes a numeric identifier for the `server.id`. The `MySqlConnector` class joins the MySQL server group so that it can read the database server's binlog, so this server ID must be [unique within all processes that make up the MySQL server group](http://dev.mysql.com/doc/refman/5.7/en/replication-howto-masterbaseconfig.html) and is any integer between 1 and (2^32)−1.

The configuration also specifies a logical name for the server, which again is included in the topic field of every source records produced by the connector and allows your application to discern the origin of those records. Our example uses a server name of "products", presumably because the database contains product information. Of course, you can name this anything meaningful to your application.

When the `MySqlConnector` class runs, it reads the MySQL server's binlog that includes all data changes and schema changes that are made to the databases hosted by the server. The connector records the schema information so that, should the connector be restarted and resume reading from the last recorded offset, it knows exactly what the database schemas looked like at that point in time. Having accurate schema information is essential to properly decode the data changes recorded in the binlog. The last two fields of our configuration specify that our connector should use the `FileDatabaseHistory` class to store database schema history changes in the `/path/to/storage/dbhistory.dat` file on the local file system (which can be named anything). 

Finally the immutable configuration is built using the `build()` method.

The next few lines of our sample code create the `EmbeddedConnector` instance, and are repeated here:

    // Create the connector with this configuration ...
	EmbeddedConnector connector = EmbeddedConnector.create()
                                                   .using(config)
                                                   .notifying(this::handleEvent)
                                                   .build();

Again, a fluent-style builder API is used to create a connector that uses our `Configuration` object and that sends all data change records to the `handleEvent(SourceRecord)` method. However, your application can reference any method that matches the signature of `java.util.function.Consumer<SourceRecord>` method, where `SourceRecord` is the `org.apache.kafka.connect.source.SourceRecord` class. However, your applications method that handles all `SourceRecord` objects produced by the connector should handle all possible errors; although any exception thrown by the method will be logged and the connector will continue operating with the next source record, your application will not have another chance to handle that particular source record.

At this point, we have an existing `EmbeddedConnector` object that is configured and ready to run, but it doesn't do anything. To execute the connector, we recommend having an `Executor` or `ExecutorService` execute the connector asynchronously:

    // Run the connector asynchronously ...
    Executor executor = ...
    executor.execute(connector);

Your application can stop the connector safely and gracefully by calling the `stop()` method on the connector:

    // At some later time ...
    connector.stop();

The connector will stop reading information from the source system, forward all remaining `SourceRecord` objects to your handler function, and flush the latest offets to offset storage. Only after all of this completes will the connector execution complete. You can optionally wait for the connector to complete. For example, you can wait for at most 30 seconds using code similar to the following:

    try {
        connector.await(30, TimeUnit.SECONDS);
    } catch ( InterruptedException e ) {
        Thread.interrupted();
    }

Your application will likely want to use the `boolean` response from `await(...)` to determine if the connector actually did complete before the 30 second timeout period, and if not to perhaps wait again. (Only if the connector is running on a daemon thread will the VM wait for the thread to complete before exiting.)

Note that care should be taken to properly stop the connector rather than simply shutdown the `ExecutorService`. Although the `EmbeddedConnector` will respond correctly when interrupted, interrupting the execution may happen during the application's handler function, which may result in inconsistent or incomplete handling of the source record. The result might be that upon restart the application receives some `SourceRecord`s that it had already processed prior to interruption. Therefore, using `stop()` is far superior since it allows the connector to gracefully complete all work and to ensure that the application never receives any duplicate source records or misses any source records.


