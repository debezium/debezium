/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.sink;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.errors.ConnectException;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.ClusterClosedEvent;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.ClusterOpeningEvent;

import io.debezium.config.Field;

public final class SinkConnection {

    // private static final String USERS_INFO = "{usersInfo: '%s', showPrivileges: true}";
    // private static final String ROLES_INFO = "{rolesInfo: '%s', showPrivileges: true, showBuiltinRoles: true}";

    public static Optional<MongoClient> canConnect(final Config config, final Field connectionStringConfigName) {
        Optional<ConfigValue> optionalConnectionString = getConfigByName(config, connectionStringConfigName.name());
        if (optionalConnectionString.isPresent() && optionalConnectionString.get().errorMessages().isEmpty()) {
            ConfigValue configValue = optionalConnectionString.get();

            AtomicBoolean connected = new AtomicBoolean();
            CountDownLatch latch = new CountDownLatch(1);
            ConnectionString connectionString = new ConnectionString((String) configValue.value());
            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .applyToClusterSettings(
                            b -> b.addClusterListener(
                                    new ClusterListener() {
                                        @Override
                                        public void clusterOpening(final ClusterOpeningEvent event) {
                                        }

                                        @Override
                                        public void clusterClosed(final ClusterClosedEvent event) {
                                        }

                                        @Override
                                        public void clusterDescriptionChanged(
                                                                              final ClusterDescriptionChangedEvent event) {
                                            ReadPreference readPreference = connectionString.getReadPreference() != null
                                                    ? connectionString.getReadPreference()
                                                    : ReadPreference.primaryPreferred();
                                            if (!connected.get()
                                                    && event.getNewDescription().hasReadableServer(readPreference)) {
                                                connected.set(true);
                                                latch.countDown();
                                            }
                                        }
                                    }))
                    .build();

            long latchTimeout = mongoClientSettings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS) + 500;
            MongoClient mongoClient = MongoClients.create(mongoClientSettings);

            try {
                if (!latch.await(latchTimeout, TimeUnit.MILLISECONDS)) {
                    configValue.addErrorMessage("Unable to connect to the server.");
                    mongoClient.close();
                }
            }
            catch (InterruptedException e) {
                mongoClient.close();
                throw new ConnectException(e);
            }

            if (configValue.errorMessages().isEmpty()) {
                return Optional.of(mongoClient);
            }
        }
        return Optional.empty();
    }

    // public static void checkUserHasActions(
    // final MongoClient client,
    // final MongoCredential credential,
    // final List<String> actions,
    // final String dbName,
    // final String collectionName,
    // final String configName,
    // final Config config) {
    //
    // if (credential == null) {
    // return;
    // }
    //
    // try {
    // Document usersInfo = client
    // .getDatabase(credential.getSource())
    // .runCommand(Document.parse(format(USERS_INFO, credential.getUserName())));
    //
    // List<String> unsupportedActions = new ArrayList<>(actions);
    // for (final Document userInfo : usersInfo.getList("users", Document.class)) {
    // unsupportedActions = removeUserActions(
    // userInfo, credential.getSource(), dbName, collectionName, actions);
    //
    // if (!unsupportedActions.isEmpty()
    // && userInfo.getList("inheritedPrivileges", Document.class, emptyList()).isEmpty()) {
    // for (final Document inheritedRole : userInfo.getList("inheritedRoles", Document.class, emptyList())) {
    // Document rolesInfo = client
    // .getDatabase(inheritedRole.getString("db"))
    // .runCommand(
    // Document.parse(format(ROLES_INFO, inheritedRole.getString("role"))));
    // for (final Document roleInfo : rolesInfo.getList("roles", Document.class, emptyList())) {
    // unsupportedActions = removeUserActions(
    // roleInfo,
    // credential.getSource(),
    // dbName,
    // collectionName,
    // unsupportedActions);
    // }
    //
    // if (unsupportedActions.isEmpty()) {
    // return;
    // }
    // }
    // }
    // if (unsupportedActions.isEmpty()) {
    // return;
    // }
    // }
    //
    // String missingPermissions = String.join(", ", unsupportedActions);
    // getConfigByName(config, configName)
    // .ifPresent(
    // c -> c.addErrorMessage(
    // format(
    // "Invalid user permissions. Missing the following action permissions: %s",
    // missingPermissions)));
    // }
    // catch (MongoSecurityException e) {
    // getConfigByName(config, configName)
    // .ifPresent(c -> c.addErrorMessage("Invalid user permissions authentication failed."));
    // }
    // catch (Exception e) {
    // LOGGER.warn("Permission validation failed due to: {}", e.getMessage(), e);
    // }
    // }

    /**
     * Checks the roles info document for matching actions and removes them from the provided list
     */
    // private static List<String> removeUserActions(final Document rolesInfo, final String authDatabase, final String databaseName, final String collectionName,
    // final List<String> userActions) {
    // List<Document> privileges = rolesInfo.getList("inheritedPrivileges", Document.class, emptyList());
    // if (privileges.isEmpty() || userActions.isEmpty()) {
    // return userActions;
    // }
    //
    // List<String> unsupportedUserActions = new ArrayList<>(userActions);
    // for (final Document privilege : privileges) {
    // Document resource = privilege.get("resource", new Document());
    // if (resource.containsKey("cluster") && resource.getBoolean("cluster")) {
    // unsupportedUserActions.removeAll(privilege.getList("actions", String.class, emptyList()));
    // }
    // else if (resource.containsKey("db") && resource.containsKey("collection")) {
    // String database = resource.getString("db");
    // String collection = resource.getString("collection");
    //
    // boolean resourceMatches = false;
    // boolean collectionMatches = collection.isEmpty() || collection.equals(collectionName);
    // if (database.isEmpty() && collectionMatches) {
    // resourceMatches = true;
    // }
    // else if (database.equals(authDatabase) && collection.isEmpty()) {
    // resourceMatches = true;
    // }
    // else if (database.equals(databaseName) && collectionMatches) {
    // resourceMatches = true;
    // }
    //
    // if (resourceMatches) {
    // unsupportedUserActions.removeAll(privilege.getList("actions", String.class, emptyList()));
    // }
    // }
    //
    // if (unsupportedUserActions.isEmpty()) {
    // break;
    // }
    // }
    //
    // return unsupportedUserActions;
    // }

    public static Optional<ConfigValue> getConfigByName(final Config config, final String name) {
        for (final ConfigValue configValue : config.configValues()) {
            if (configValue.name().equals(name)) {
                return Optional.of(configValue);
            }
        }
        return Optional.empty();
    }

    private SinkConnection() {
    }
}
