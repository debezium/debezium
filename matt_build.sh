#jenv local  openjdk64-11.0.2 # openjdk64-1.8.0.232 - i think debezium upgraded recently
#jenv disable-plugin maven
#jenv enable-plugin maven

#mvn clean install -pl :debezium-ide-configs,:debezium-checkstyle,:debezium-revapi
#
#mvn clean install -pl :debezium-ide-configs,:debezium-checkstyle,:debezium-revapi,:debezium-connector-mysql -am -P quick

# mvn clean install -pl :debezium-ide-configs,:debezium-checkstyle,:debezium-revapi,:debezium-ddl-parser -am

mvn install -pl :debezium-ide-configs,:debezium-checkstyle,:debezium-revapi,:debezium-ddl-parser,:debezium-core,:debezium-connector-mysql -am -P assembly -DskipLongRunningTests=true -DskipTests=true -DskipITs=true

# ls debezium-connector-mysql/target/debezium-connector-mysql-1.9.0-SNAPSHOT-plugin.tar.gz

aws s3 cp debezium-connector-mysql/target/debezium-connector-mysql-1.9.0-SNAPSHOT-plugin.tar.gz s3://maven-public/snapshot/io/debezium/debezium-connector-mysql/1.9.0-SNAPSHOT/

aws s3api put-object-acl --bucket maven-public --key snapshot/io/debezium/debezium-connector-mysql/1.9.0-SNAPSHOT/debezium-connector-mysql-1.9.0-SNAPSHOT-plugin.tar.gz --acl public-read


# samples from my history commands... might be useful?

# aws s3 cp debezium-connector-mysql-1.7.0-SNAPSHOT-plugin.tar.gz s3://dev.rytebox.net/
# curl -k -SL "https://s3.amazonaws.com/dev.rytebox.net/debezium-connector-mysql-1.7.0-SNAPSHOT-plugin.tar.gz" -o /tmp/connector.gz
# aws s3 cp debezium-connector-mysql-1.7.0-RBX-SNAPSHOT.jar s3://maven-repo-rytebox.axispoint.com/ !! need to resolve private permissions

# curl -XPOST http://localhost:8083/connectors -d @mappings-debezium-source.json -H  "Content-Type: application/json"
# curl -XPUT http://localhost:8083/connectors/mappings-connector -d @mappings-debezium-source.json -H  "Content-Type: application/json"
# curl -s -X PUT -H "Content-Type:application/json" http://localhost:8083/admin/loggers/io.debezium.connector.mysql -d '{"level": "DEBUG"}'
