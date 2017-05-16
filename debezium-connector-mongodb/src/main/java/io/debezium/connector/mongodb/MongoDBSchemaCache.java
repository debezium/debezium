package io.debezium.connector.mongodb;


import com.datapipeline.clients.mongodb.MongodbSchema;
import com.datapipeline.clients.mongodb.MongodbSchemaConfig;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MongoDBSchemaCache {

    private Map<CollectionInfo,List<MongodbSchema>> schemaMap = new ConcurrentHashMap<>();

    public MongoDBSchemaCache(List<MongodbSchemaConfig> mongodbSchemaConfigs){
        for(MongodbSchemaConfig config : mongodbSchemaConfigs){
            schemaMap.put(new CollectionInfo(config.getDbName(),config.getCollectionName()), config.getMongodbSchemaList());
        }
    }

    public List<MongodbSchema> getMongodbSchemasForCollection(String dbName, String collectionName){
        return schemaMap.get(new CollectionInfo(dbName,collectionName));
    }

    private class CollectionInfo{

        private final String dbName;

        private final String collectionName;

        private CollectionInfo(String dbName, String collectionName) {
            this.dbName = dbName;
            this.collectionName = collectionName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getCollectionName() {
            return collectionName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CollectionInfo that = (CollectionInfo) o;

            if (!dbName.equals(that.dbName)) return false;
            return collectionName.equals(that.collectionName);
        }

        @Override
        public int hashCode() {
            int result = dbName.hashCode();
            result = 31 * result + collectionName.hashCode();
            return result;
        }
    }
}
