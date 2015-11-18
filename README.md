
== Requirements

The following software is required to build Debezium locally:

* [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or [OpenJDK 8](http://openjdk.java.net/projects/jdk8/)
* [Maven 3.2.1](https://maven.apache.org/index.html) or later
* [Docker Engine 1.4](http://docs.docker.com/engine/installation/) or later

See the links above for installation instructions on your platform.

=== Docker

Many open source software projects use Java and Maven, but requiring Docker is less common. Debezium is designed to talk to a number of external systems, such as various databases and services, and our integration tests verify Debezium does this correctly. But rather than expect you have all of these software systems installed locally, Debezium's build system uses Docker to automatically download or create the necessary images and start containers for each of the systems. The integration tests can then use these services and verify Debezium behaves as expected, and when the integration tests finish, Debezum's build will automatically stop any containers that it started.

Debezium also has a few modules that are not written in Java, and so they have to be required on the target operating system. Docker lets our build do this using images with the target operating system(s) and all necessary development tools.

Using Docker has several advantages:

1. You don't have to install, configure, and run specific versions of each external services on your local machine, or have access to them on your local network. Even if you do, Debezium's build won't use them.
1. We can test multiple versions of an external service. Each module can start whatever containers it needs, so different modules can easily use different versions of the services.
1. Everyone can run complete builds locally. You don't have to rely upon a remote continuous integration server running the build in an environment set up with all the required services. 
1. All builds are consistent. When multiple developers each build the same codebase, they should see exactly the same results -- as long as they're using the same or equivalent JDK, Maven, and Docker versions. That's because the containers will be running the same versions of the services on the same operating systems. Plus, all of the tests are designed to connect to the systems running in the containers, so nobody has to fiddle with connection properties or custom configurations specific to their local environments.
1. No need to clean up the services, even if those services modify and store data locally. Docker *images* are cached, so building them reusing them to start containers is fast and consistent. However, Docker *containers* are never reused: they always start in their pristine initial state, and are discarded when they are shutdown. Integration tests rely upon containers, and so cleanup is handled automatically.

