// CREATE DBZ USER
db = db.getSiblingDB('admin');
db.runCommand({
    createRole: "listDatabases",
    privileges: [
        { resource: { cluster : true }, actions: ["listDatabases"]}
    ],
    roles: []
});

db.runCommand({
    createRole: "readChangeStream",
    privileges: [
        { resource: { db: "", collection: ""}, actions: [ "find", "changeStream" ] }
    ],
    roles: []
});
db = db.getSiblingDB('$external');
db.runCommand({
    createUser: '${userName}',
    roles: [
        { role: "listDatabases", db: "admin" },
        { role: "readChangeStream", db: "admin" },
        { role: "readAnyDatabase", db: "admin" },
        { role: "read", db: "config", collection: "shards" },
        { role: "read", db: "local"}
    ],
    writeConcern: { w: 'majority' , wtimeout: 5000 }
});