package io.debezium.connector.mongodb;

import com.datapipeline.base.mongodb.MongodbSchemaNameConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

public class DpTopicSelector implements TopicSelector {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final String DELIMITER = ".";

    private String prefix;

    private final Map<String, String> collectionSchemaNameMap = new ConcurrentHashMap<>();

    public DpTopicSelector(List<MongodbSchemaNameConfig> schemaNameConfigs, String prefix){
        if(prefix != null && prefix.trim().length() > 0){
            this.prefix = prefix.trim();
        }
        if(schemaNameConfigs != null && !schemaNameConfigs.isEmpty()){
            for(MongodbSchemaNameConfig schemaNameConfig : schemaNameConfigs){
                for(String collectionName : schemaNameConfig.getCollectionNames()) {
                    collectionSchemaNameMap.put(collectionName, schemaNameConfig.getSchemaName());
                }
            }
            return;
        }
    }

    @Override
    public String getTopic(CollectionId collectionId) {
        StringJoiner sb = new StringJoiner(DELIMITER);
        if(prefix != null){
            sb.add(prefix);
        }
        sb.add(collectionId.dbName());
        String schemaName = collectionSchemaNameMap.get(collectionId.name());
        if(schemaName != null){
            sb.add(schemaName);
        }else {
            sb.add(collectionId.name());
        }
        return sb.toString();
    }
}
