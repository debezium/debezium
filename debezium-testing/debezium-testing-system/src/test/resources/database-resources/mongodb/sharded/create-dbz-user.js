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

db.createUser({
    user: 'debezium',
    pwd: 'dbz',
    roles: [
        { role: "listDatabases", db: "admin" },
        { role: "readChangeStream", db: "admin" },
        { role: "readAnyDatabase", db: "admin" },
        { role: "readWriteAnyDatabase", db: "admin" }
    ]
});
