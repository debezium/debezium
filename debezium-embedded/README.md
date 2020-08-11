# Embedding Debezium connectors in applications

Debezium connectors are normally operated by deploying them to a Kafka Connect service, and configuring one or more connectors to monitor upstream databases and produce data change events for all changes that they sees in the upstream databases. Those data change events are written to Kafka, where they can be independently consumed by many different applications. Kafka Connect provides excellent fault tolerance and scalability, since it runs as a distributed service and ensures that all registered and configured connectors are always running. For example, even if one of the Kafka Connect endpoints in a cluster goes down, the remaining Kafka Connect endpoints will restart any connectors that were previously running on the now-terminated endpoint, minimizing downtime and eliminating administrative activities.

Not every application needs this level of fault tolerance and reliability, and they may not want to rely upon an external cluster of Kafka brokers and Kafka Connect services. Instead, some applications would prefer to *embed* Debezium connectors directly within the application space. They still want the same data change events, but prefer to have the connectors send them directly to the application rather than persiste them inside Kafka.

This `debezium-embedded` module defines a small library that allows an application to easily configure and run Debezium connectors.

## Dependencies

To use this module, add the `debezium-embedded` module to your application's dependencies. For Maven, this entails adding the following to your application's POM:

    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-embedded</artifactId>
        <version>${version.debezium}</version>
    </dependency>

where `${version.debezium}` is either the version of Debezium you're using or a Maven property whose value contains the Debezium version string.

Likewise, add dependencies for each the Debezium connectors that your application will use. For example, the following can be added to your application's Maven POM file so your application can use the MySQL connector:

    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-connector-mysql</artifactId>
        <version>${version.debezium}</version>
    </dependency>

## In the code

Your application needs to set up an embedded engine for each connector instance you want to run. The `io.debezium.embedded.EmbeddedEngine` class serves as an easy-to-use wrapper around any standard Kafka Connect connector and completely manages the connector's lifecycle. Basically, you create the `EmbeddedEngine` with a configuration (perhaps loaded from a properties file) that defines the environment for both the engine and the connector. You also provide the engine with a function that the it will call for every data change event produced by the connector.

Here's an example of code that configures and runs an embedded MySQL connector:

    // Define the configuration for the embedded and MySQL connector ...
    Configuration config = Configuration.create()
                                        /* begin engine properties */
                                        .with("name", "my-sql-connector")
                                        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                                        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                                        .with("offset.storage.file.filename", "/path/to/storage/offset.dat")
                                        .with("offset.flush.interval.ms", 60000)
                                        /* begin connector properties */
                                        .with("database.hostname", "localhost")
                                        .with("database.port", 3306)
                                        .with("database.user", "mysqluser")
                                        .with("database.password", "mysqlpw")
                                        .with("server.id", 85744)
                                        .with("server.name", "my-app-connector")
                                        .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                                        .with("database.history.file.filename", "/path/to/storage/dbhistory.dat")
                                        .build())

    // Create the engine with this configuration ...
	EmbeddedEngine engine = EmbeddedEngine.create()
                                          .using(config)
                                          .notifying(this::handleEvent)
                                          .build();
    
    // Run the engine asynchronously ...
    Executor executor = ...
    executor.execute(engine);

    // At some later time ...
    engine.stop();

Let's look into this code in more detail, starting with the first few lines that we repeat here:

    // Define the configuration for the embedded and MySQL connector ...
    Configuration config = Configuration.create()
                                        /* begin engine properties */
                                        .with("name", "mysql-connector")
                                        .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                                        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                                        .with("offset.storage.file.filename", "/path/to/storage/offset.dat")
                                        .with("offset.flush.interval.ms", 60000)

This creates a new `Configuration` object and uses a fluent-style builder API to set several fields required by the engine regardless of which connector is being used. The first is a name for the engine that will be used within the source records produced by the connector and its internal state, so use something meaningful in your application. The `connector.class` field defines the name of the class that extends the Kafka Connect `org.apache.kafka.connect.source.SourceConnector` abstract class; in this example, we specify Debezium's `MySqlConnector` class.

When a Kafka Connect connector runs, it reads information from the source and periodically records "offsets" that define how much of that information it has processed. Should the connector be restarted, it will use the last recorded offset to know where in the source information it should resume reading. Since connectors don't know or care *how* the offsets are stored, it is up to the engine to provide a way to store and recover these offsets. The next few fields of our configuration specify that our engine should use the `FileOffsetBackingStore` class to store offsets in the `/path/to/storage/offset.dat` file on the local file system (the file can be named anything and stored anywhere). Additionally, although the connector records the offsets with every source record it produces, the engine flushes the offsets to the backing store periodically (in our case, once each minute). These fields can be tailored as needed for your application.

The next few lines define the fields that are specific to the connector, which in our example is the `MySqlConnector` connector:

                                        /* begin connector properties */
                                        .with("database.hostname", "localhost")
                                        .with("database.port", 3306)
                                        .with("database.user", "mysqluser")
                                        .with("database.password", "mysqlpw")
                                        .with("server.id", 85744)
                                        .with("server.name", "products")
                                        .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                                        .with("database.history.file.filename", "/path/to/storage/dbhistory.dat")
                                        .build())

Here, we set the name of the host machine and port number where the MySQL database server is running, and we define the username and password that will be used to connect to the MySQL database. Note that for MySQL the username and password should correspond to a MySQL database user that has been granted the [`REPLICATION SLAVE` privilege](http://dev.mysql.com/doc/refman/5.7/en/replication-howto-repuser.html), allowing the database to read the server's binlog that is normally used for MySQL replication.

The configuration also includes a numeric identifier for the `server.id`. Since MySQL's binlog is part of the MySQL replication mechanism, in order to read the binlog the `MySqlConnector` instance must join the MySQL server group, and that means this server ID must be [unique within all processes that make up the MySQL server group](http://dev.mysql.com/doc/refman/{mysql-version}/en/replication-howto-masterbaseconfig.html) and is any integer between 1 and (2^32)−1. In our code we set it to a fairly large but somewhat random value we'll use only for our application.

The configuration also specifies a logical name for the MySQL server. The connector includes this logical name within the topic field of every source record it produces, enabling your application to discern the origin of those records. Our example uses a server name of "products", presumably because the database contains product information. Of course, you can name this anything meaningful to your application.

When the `MySqlConnector` class runs, it reads the MySQL server's binlog, which includes all data changes and schema changes made to the databases hosted by the server. Since all changes to data are structured in terms of the owning table's schema at the time the change was recorded, the connector needs to track all of the schema changes so that it can properly decode the change events. The connector records the schema information so that, should the connector be restarted and resume reading from the last recorded offset, it knows exactly what the database schemas looked like at that offset. How the connector records the database schema history is defined in the last two fields of our configuration, namely that our connector should use the `FileDatabaseHistory` class to store database schema history changes in the `/path/to/storage/dbhistory.dat` file on the local file system (again, this file can be named anything and stored anywhere). 

Finally the immutable configuration is built using the `build()` method. (Incidentally, rather than build it programmatically, we could have *read* the configuration from a properties file using one of the `Configuration.read(...)` methods.)

Now that we have a configuration, we can create our engine. Here again are the relevant lines of code:

    // Create the engine with this configuration ...
    EmbeddedEngine engine = EmbeddedEngine.create()
                                          .using(config)
                                          .notifying(this::handleEvent)
                                          .build();

A fluent-style builder API is used to create an engine that uses our `Configuration` object and that sends all data change records to the `handleEvent(SourceRecord)` method, which can be any method that matches the signature of the `java.util.function.Consumer<SourceRecord>` functional interface, where `SourceRecord` is the `org.apache.kafka.connect.source.SourceRecord` class. Note that your application's handler function should not throw any exceptions; if it does, the engine will log any exception thrown by the method and will continue to operate on the next source record, but your application will not have another chance to handle the particular source record that caused the exception, meaning your application might become inconsistent with the database. 

At this point, we have an existing `EmbeddedEngine` object that is configured and ready to run, but it doesn't do anything. The `EmbeddedEngine` is designed to be executed asynchronously by an `Executor` or `ExecutorService`:

    // Run the engine asynchronously ...
    Executor executor = ...
    executor.execute(engine);

Your application can stop the engine safely and gracefully by calling its `stop()` method:

    // At some later time ...
    engine.stop();

The engine's connector will stop reading information from the source system, forward all remaining `SourceRecord` objects to your handler function, and flush the latest offets to offset storage. Only after all of this completes will the engine's `run()` method return. If your application needs to wait for the engine to completely stop before exiting, you can do this with the engine's `await(...)` method:

    try {
        while (!connector.await(30, TimeUnit.SECONDS)) {
            logger.info("Wating another 30 seconds for the embedded enging to shut down");            
        }
    } 
    catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
    }

Recall that when the JVM shuts down, it only waits for daemon threads. Therefore, if your application exits, be sure to wait for completion of the engine or alternatively run the engine on a daemon thread.

Your application should always properly stop the engine to ensure graceful and complete shutdown and that each source record is sent to the application exactly one time. For example, do not rely upon shutting down the `ExecutorService`, since that interrupts the running threads. Although the `EmbeddedEngine` will indeed terminate when its thread is interrupted, the engine may not terminate cleanly, and when your application is restarted it may see some of the same source records that it had processed just prior to the shutdown.

## Handling failures

When the engine executes, its connector is actively recording the source offset inside each source record, and the engine is periodically flushing those offsets to persistent storage. When the application and engine shutdown normally or crash, when they are restarted the engine and its connector will resume reading the source information *from the last recorded offset*. 

So, what happens when your application fails while an embedded engine is running? The net effect is that the application will likely receive some source records after restart that it had already processed right before the crash. How many depends upon how frequently the engine flushes offsets to its store (via the `offset.flush.interval.ms` property) and how many source records the specific connector returns in one batch. The best case is that the offsets are flushed every time (e.g., `offset.flush.interval.ms` is set to 0), but even then the embedded engine will still only flush the offsets after each batch of source records is received from the connector.

For example, the MySQL connector uses the `max.batch.size` to specify the maximum number of source records that can appear in a batch. Even with `offset.flush.interval.ms` is set to 0, when an application restarts after a crash it may see up to *n* duplicates, where *n* is the size of the batches. If the `offset.flush.interval.ms` property is set higher, then the application may see up to `n * m` duplicates, where *n* is the maximum size of the batches and *m* is the number of batches that might accumulate during a single offset flush interval. (Obviously it is possible to configure embedded connectors to use no batching and to always flush offsets, resulting in an application never receiving any duplicate source records. However, this dramatically increases the overhead and decreases the throughput of the connectors.)

The bottom line is that when using embedded connectors, applications will receive each source record exactly once during normal operation (including restart after a graceful shutdown), but do need to be tolerant of receiving duplicate events immediately following a restart after a crash or improper shutdown. If applications need more rigorous exactly-once behavior, then they should use the full Debezium platform that can provide exactly-once guarantees (even after crashes and restarts).

