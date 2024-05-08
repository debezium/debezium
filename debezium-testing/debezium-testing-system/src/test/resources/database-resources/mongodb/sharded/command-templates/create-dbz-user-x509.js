// CREATE DBZ USER
db = db.getSiblingDB('admin');
db.runCommand({
    createRole: "listDatabases_x509",
    privileges: [
        { resource: { cluster : true }, actions: ["listDatabases"]}
    ],
    roles: []
});

db.runCommand({
    createRole: "readChangeStream_x509",
    privileges: [
        { resource: { db: "", collection: ""}, actions: [ "find", "changeStream" ] }
    ],
    roles: []
});
db = db.getSiblingDB('$external');
db.runCommand({
    createUser: '${userName}',
    roles: [
        { role: "listDatabases_x509", db: "admin" },
        { role: "readChangeStream_x509", db: "admin" },
        { role: "readAnyDatabase", db: "admin" },
        { role: "read", db: "config", collection: "shards" },
        { role: "read", db: "local"}
    ],
    writeConcern: { w: 'majority' , wtimeout: 5000 }
});