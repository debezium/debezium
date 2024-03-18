# YugabyteDB Development Instructions

## Compiling code

The following command will quickly build the postgres connector code with all the required dependencies and proto files:

```bash
./mvnw clean install -Dquick -pl debezium-connector-postgres -am 
```