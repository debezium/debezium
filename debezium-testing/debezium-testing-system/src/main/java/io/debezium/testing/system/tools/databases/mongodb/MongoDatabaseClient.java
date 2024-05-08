/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.testing.system.tools.databases.mongodb;

import static io.debezium.testing.system.tools.WaitConditions.scaled;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import io.debezium.testing.system.tools.databases.Commands;
import io.debezium.testing.system.tools.databases.DatabaseClient;

/**
 *
 * @author Jakub Cechacek
 */
public class MongoDatabaseClient implements DatabaseClient<MongoClient, RuntimeException> {

    private final String url;
    private final String username;
    private final String password;
    private final String authSource;

    public MongoDatabaseClient(String url, String username, String password, String authSource) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.authSource = authSource;
    }

    private boolean doExecute(Commands<MongoClient, RuntimeException> commands) throws RuntimeException {
        MongoCredential credential = MongoCredential.createCredential(username, authSource, password.toCharArray());
        ConnectionString connString = new ConnectionString(url);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .credential(credential)
                .retryWrites(true)
                .build();

        MongoClient client = MongoClients.create(settings);
        commands.execute(client);
        return true;
    }

    public void execute(Commands<MongoClient, RuntimeException> commands) throws RuntimeException {
        await()
                .atMost(scaled(2), TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> doExecute(commands));
    }

    public void execute(String database, Commands<MongoDatabase, RuntimeException> commands) {
        execute(con -> {
            MongoDatabase db = con.getDatabase(database);
            commands.execute(db);
        });
    }

    public void execute(
                        String database, String collection, Commands<MongoCollection<Document>, RuntimeException> commands) {
        execute(database, db -> {
            MongoCollection<Document> col = db.getCollection(collection);
            commands.execute(col);
        });
    }
}
