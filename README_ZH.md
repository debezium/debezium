[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-parent/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302529-users)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://debezium.zulipchat.com/#narrow/stream/302533-dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
The Antlr grammars within the debezium-ddl-parser module are licensed under the [MIT License](https://opensource.org/licenses/MIT).

中文 | [English](README.md) | [Japanese](README_JA.md) | [Korean](README_KO.md)

# Debezium 简介


Debezium是一个开源项目，为捕获数据更改(change data capture,CDC)提供了一个低延迟的流式处理平台。你可以安装并且配置Debezium去监控你的数据库，然后你的应用就可以消费对数据库的每一个行级别(row-level)的更改。只有已提交的更改才是可见的，所以你的应用不用担心事务(transaction)或者更改被回滚(roll back)。Debezium为所有的数据库更改事件提供了一个统一的模型，所以你的应用不用担心每一种数据库管理系统的错综复杂性。另外，由于Debezium用持久化的、有副本备份的日志来记录数据库数据变化的历史，因此，你的应用可以随时停止再重启，而不会错过它停止运行时发生的事件，保证了所有的事件都能被正确地、完全地处理掉。

监控数据库，并且在数据变动的时候获得通知一直是很复杂的事情。关系型数据库的触发器可以做到，但是只对特定的数据库有效，而且通常只能更新数据库内的状态(无法和外部的进程通信)。一些数据库提供了监控数据变动的API或者框架，但是没有一个标准，每种数据库的实现方式都是不同的，并且需要大量特定的知识和理解特定的代码才能运用。确保以相同的顺序查看和处理所有更改，同时最小化影响数据库仍然非常具有挑战性。

Debezium提供了模块为你做这些复杂的工作。一些模块是通用的，并且能够适用多种数据库管理系统，但在功能和性能方面仍有一些限制。另一些模块是为特定的数据库管理系统定制的，所以他们通常可以更多地利用数据库系统本身的特性来提供更多功能。

## Debezium基础架构

Debezium是一个捕获数据更改(CDC)平台，并且利用Kafka和Kafka Connect实现了自己的持久性、可靠性和容错性。每一个部署在Kafka Connect分布式的、可扩展的、容错性的服务中的connector监控一个上游数据库服务器，捕获所有的数据库更改，然后记录到一个或者多个Kafka topic(通常一个数据库表对应一个kafka topic)。Kafka确保所有这些数据更改事件都能够多副本并且总体上有序(Kafka只能保证一个topic的单个分区内有序)，这样，更多的客户端可以独立消费同样的数据更改事件而对上游数据库系统造成的影响降到很小(如果N个应用都直接去监控数据库更改，对数据库的压力为N，而用debezium汇报数据库更改事件到kafka，所有的应用都去消费kafka中的消息，可以把对数据库的压力降到1)。另外，客户端可以随时停止消费，然后重启，从上次停止消费的地方接着消费。每个客户端可以自行决定他们是否需要exactly-once或者at-least-once消息交付语义保证，并且所有的数据库或者表的更改事件是按照上游数据库发生的顺序被交付的。
 
对于不需要或者不想要这种容错级别、性能、可扩展性、可靠性的应用，他们可以使用内嵌的Debezium connector引擎来直接在应用内部运行connector。这种应用仍需要消费数据库更改事件，但更希望connector直接传递给它，而不是持久化到Kafka里。
## 常见使用场景

Debezium有很多非常有价值的使用场景，我们在这儿仅仅列出几个更常见的使用场景。

### 缓存失效(Cache invalidation)

在缓存中缓存的条目(entry)在源头被更改或者被删除的时候立即让缓存中的条目失效。如果缓存在一个独立的进程中运行(例如Redis，Memcache，Infinispan或者其他的)，那么简单的缓存失效逻辑可以放在独立的进程或服务中，从而简化主应用的逻辑。在一些场景中，缓存失效逻辑可以更复杂一点，让它利用更改事件中的更新数据去更新缓存中受影响的条目。
### 简化单体应用(Simplifying monolithic applications)

许多应用更新数据库，然后在数据库中的更改被提交后，做一些额外的工作：更新搜索索引，更新缓存，发送通知，运行业务逻辑，等等。这种情况通常称为双写(dual-writes)，因为应用没有在一个事务内写多个系统。这样不仅应用逻辑复杂难以维护，而且双写容易丢失数据或者在一些系统更新成功而另一些系统没有更新成功的时候造成不同系统之间的状态不一致。使用捕获更改数据技术(change data capture,CDC)，在源数据库的数据更改提交后，这些额外的工作可以被放在独立的线程或者进程(服务)中完成。这种实现方式的容错性更好，不会丢失事件，容易扩展，并且更容易支持升级。

### 共享数据库(Sharing databases)

 当多个应用共用同一个数据库的时候，一个应用提交的更改通常要被另一个应用感知到。一种实现方式是使用消息总线，尽管非事务性(non-transactional)的消息总线总会受上面提到的双写(dual-writes)影响。但是，另一种实现方式，即Debezium，变得很直接：每个应用可以直接监控数据库的更改，并且响应更改。

### 数据集成(Data integration)

数据通常被存储在多个地方，尤其是当数据被用于不同的目的的时候，会有不同的形式。保持多系统的同步是很有挑战性的，但是可以通过使用Debezium加上简单的事件处理逻辑来实现简单的ETL类型的解决方案。

### 命令查询职责分离(CQRS)

在命令查询职责分离 [Command Query Responsibility Separation (CQRS)](http://martinfowler.com/bliki/CQRS.html) 架构模式中，更新数据使用了一种数据模型，读数据使用了一种或者多种数据模型。由于数据更改被记录在更新侧(update-side)，这些更改将被处理以更新各种读展示。所以CQRS应用通常更复杂，尤其是他们需要保证可靠性和全序(totally-ordered)处理。Debezium和CDC可以使这种方式更可行：写操作被正常记录，但是Debezium捕获数据更改，并且持久化到全序流里，然后供那些需要异步更新只读视图的服务消费。写侧(write-side)表可以表示面向领域的实体(domain-oriented entities)，或者当CQRS和 [Event Sourcing](http://martinfowler.com/eaaDev/EventSourcing.html) 结合的时候，写侧表仅仅用做追加操作命令事件的日志。

## Building Debezium

使用Debezium代码库并在本地配置它需要以下软件：

* [Git](https://git-scm.com) 2.2.1 or later
* JDK 17 or later, e.g. [OpenJDK](http://openjdk.java.net/projects/jdk/)
* [Apache Maven](https://maven.apache.org/index.html) 3.8.4
* [Docker Engine](https://docs.docker.com/engine/install/) or [Docker Desktop](https://docs.docker.com/desktop/) 1.9 or later

有关平台上的安装说明，请参阅上面的链接。您可以通过以下指令查看安装版本

    $ git --version
    $ javac -version
    $ mvn -version
    $ docker --version

### 为什么选用 Docker?

许多开源软件项目使用Git、Java和Maven，但需要Docker的情况不太常见。Debezium被设计用来与许多外部系统进行通信，比如各种数据库和服务，我们的集成测试验证了Debezium成功地做到了这一点。但Debezium的构建系统使用Docker自动下载或创建必要的镜像，并为每个系统启动容器，而不是期望您在本地安装所有这些软件系统。然后，集成测试可以使用这些服务并验证Debezium的行为是否符合预期，当集成测试完成时，Debezium将自动停止它启动的所有容器.

Debezium还有一些不是用Java编写的模块，而且这些模块在目标操作系统上是必须的。通过Docker，我们可以使用带有目标操作系统以及所有必要开发工具的镜像来构建它们。

使用Docker有几个优点：


1. 不需要在本地计算机上安装、配置和运行每个所依赖的外部服务的特定版本，也不必在本地网络上访问它们。即使配置了，Debezium也不会用到它们。
2. 我们可以测试外部服务的多个版本。每个模块可以启动它需要的任何容器，因此不同的模块可以轻松地使用不同版本的服务。
3. 每个人都可以在本地运行完整的构建。 不必依赖安装了所有必需服务的远程CI服务器来运行构建。
4. 所有构建都是一致的。当多个开发人员各自构建相同的代码库时，他们应该看到完全相同的结果——只要他们使用相同或等效的JDK、Maven和Docker版本。 这是因为容器将在相同的操作系统上运行相同版本的服务。另外，所有的测试都被设计为连接运行在容器中的系统，因此没有人需要修改连接属性或特定于其本地环境的自定义配置。
5. 不需要清理服务, 即使这些服务在本地修改和存储数据. Docker *镜像* 是可缓存的, 重用镜像可以快速启动容器并保持一致性, 但是Docker *容器* 永远不会被重用：它们总是在初始状态下启动，在关闭时丢弃。集成测试依赖容器，因此会自动清理容器。

### 配置Docker环境

Docker Maven插件通过检查以下环境变量来解析Docker主机：

    export DOCKER_HOST=tcp://10.1.2.2:2376
    export DOCKER_CERT_PATH=/path/to/cdk/.vagrant/machines/default/virtualbox/.docker
    export DOCKER_TLS_VERIFY=1

使用Docker Machine或类似软件时会自动设置这些环境变量。
### 项目编译

首先从Git仓库获取代码：

    $ git clone https://github.com/debezium/debezium.git
    $ cd debezium

然后用maven构建项目

    $ mvn clean install

这行命令会启动构建，并为不同的dbms使用不同的Docker容器。注意，如果未运行或未配置Docker，可能会出现奇怪的错误——如果遇到这种情况，一定要检查Docker是否正在运行，比如可以使用`Docker ps`列出运行中的容器。

### 本地没有Docker?

可以使用以下命令跳过集成测试和docker的构建：

    $ mvn clean install -DskipITs

### 仅构建工件（artifacts），不运行测试、代码风格检查等其他插件

可以使用“quick“构建选项来跳过所有非必须的插件，例如测试、集成测试、代码风格检查、格式化、API兼容性检查等：

    $ mvn clean verify -Dquick

这行命令是构建工件（artifacts）最快的方法，但它不会运行任何与质量保证（QA）相关的Maven插件。这在需要尽快构建connector jar包、归档时可以派上用场，比如需要在Kafka Connect中进行手动测试。

### 使用wal2json或 pgoutput logical decoding plug-ins 运行Postgres connector的测试

Postgres connector支持三个用于从数据库服务器捕获流式数据更改的逻辑解码插件：decoderbufs（默认）、wal2json以及pgoutput。运行PG connector的集成测试时，如果要使用wal2json，需要启用“wal2json decoder”构建配置：

    $ mvn clean install -pl :debezium-connector-postgres -Pwal2json-decoder
    
要使用pgoutput，需要启用“pgoutput decoder”和“postgres-10”构建配置：

    $ mvn clean install -pl :debezium-connector-postgres -Ppgoutput-decoder,postgres-10

在使用wal2json插件时，一些测试目前无法通过。 通过查找`io.debezium.connector.postgresql.DecoderDifferences`中定义的类型的引用，可以找到这些测试。

### 使用指定Apicurio版本运行Postgres connector测试

如果要使用带有指定版本Apicurio的wal2json或pgoutput逻辑解码插件运行PG connector测试，可以像这样传递测试参数：

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder -Ddebezium.test.apicurio.version=1.3.1.Final

如果没有设置该参数，将自动获取并设置该参数为Apicurio的稳定版本。

### 对外部数据库运行Postgres connector测试, 例如：Amazon RDS
如果要对非RDS集群进行测试，请注意`<your user>`必须是超级用户，不仅要具有`复制`权限，还要有登录`pg_hba.conf`中`所有`数据库的权限。还要求目标服务器上必须有`postgis`包，才能通过某些测试。

    $ mvn clean install -pl debezium-connector-postgres -Pwal2json-decoder \
         -Ddocker.skip.build=true -Ddocker.skip.run=true -Dpostgres.host=<your PG host> \
         -Dpostgres.user=<your user> -Dpostgres.password=<your password> \
         -Ddebezium.test.records.waittime=10

超时时间可以根据需要进行调整。

有关在RDS上设置要测试的数据库的详细信息，请参阅 [PostgreSQL on Amazon RDS](debezium-connector-postgres/RDS.md) 

### 使用Oracle XStream运行Oracle connector测试

    $ mvn clean install -pl debezium-connector-oracle -Poracle-xstream,oracle-tests -Dinstantclient.dir=<path-to-instantclient>

### 使用非CDB数据库运行Oracle connector测试

    $ mvn clean install -pl debezium-connector-oracle -Poracle-tests -Dinstantclient.dir=<path-to-instantclient> -Ddatabase.pdb.name=

### 使用IDE中的oplog捕获运行MongoDB测试

不使用maven运行测试时，需要确保传递了正确的执行参数。可以在`.github/workflows/mongodb-oplog-workflow.yml`中查正确参数，添加`debezium.test`前缀后，再将这些参数添加到JVM执行参数之后。由于测试运行在Maven生命周期之外，还需要手动启动MongoDB connector目录下的MongoDB镜像:

    $ mvn docker:start -B -am -Passembly -Dcheckstyle.skip=true -Dformat.skip=true -Drevapi.skip -Dcapture.mode=oplog -Dversion.mongo.server=3.6 -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -Dcapture.mode=oplog -Dmongo.server=3.6

执行测试命令行的相关部分应该如下：

    java -ea -Ddebezium.test.capture.mode=oplog -Ddebezium.test.version.mongo.server=3.6 -Djava.awt.headless=true -Dconnector.mongodb.members.auto.discover=false -Dconnector.mongodb.name=mongo1 -DskipLongRunningTests=true [...]


## 贡献源码(Contributing)

Debezium社区欢迎所有愿意提供帮助的人，无论是报告问题，帮助撰写文档，还是提供代码用于修复错误、添加测试或实现新功能。有关详细信息，请参阅本[文档](CONTRIBUTE.md)。

非常感谢所有Debezium贡献者！

<a href="https://github.com/debezium/debezium/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=debezium/debezium" />
</a>
