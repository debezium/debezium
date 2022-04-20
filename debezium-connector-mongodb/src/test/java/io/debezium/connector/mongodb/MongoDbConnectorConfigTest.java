package io.debezium.connector.mongodb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;
import java.util.Optional;

public class MongoDbConnectorConfigTest {

    @Test
    public void parseSignallingMessage() {
        Schema schema = new SchemaBuilder(Schema.Type.STRUCT).field("after", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("after", "{\"_id\":\"test-1\"," +
                            "\"type\":\"execute-snapshot\"," +
                            "\"data\":{\"data-collections\":[\"database.collection\"],\"type\":\"incremental\"}}");
        MongoDbConnectorConfig mongoDbConnectorConfig = new MongoDbConnectorConfig(TestHelper.getConfiguration());

        Optional<String[]> resultOpt = mongoDbConnectorConfig.parseSignallingMessage(struct);

        Assert.assertTrue(resultOpt.isPresent());
        String[] result = resultOpt.get();
        Assert.assertEquals(3, result.length);
        Assert.assertEquals("test-1", result[0]);
        Assert.assertEquals("execute-snapshot", result[1]);
        Assert.assertEquals("{\"data-collections\": [\"database.collection\"], \"type\": \"incremental\"}", result[2]);
    }
}
