// SETUP SHARDING
sh.addShard("shard1rs/mongo-shard1r1.${ocp.project.mongo}.svc.cluster.local:27018");
sh.addShard("shard2rs/mongo-shard2r1.${ocp.project.mongo}.svc.cluster.local:27018");

db = db.getSiblingDB('inventory');
sh.enableSharding("inventory");
db.createCollection(
   "customers",
   { clusteredIndex: { "key": { _id: 1 }, "unique": true, "name": "customers clustered key" } }
);
sh.shardCollection("inventory.customers", { _id: 1 } );
sh.shardCollection("inventory.products", { _id: "hashed" } );

sh.addShardToZone("shard1rs", "ONE");
sh.addShardToZone("shard2rs", "TWO");

sh.updateZoneKeyRange(
   "inventory.customers",
   { _id : 1000 },
   { _id : 1003 },
   "ONE"
);
sh.updateZoneKeyRange(
   "inventory.customers",
   { _id : 1003 },
   { _id : 1004 },
   "TWO"
);

// CREATE TEST DATA
db.products.insert([
    { _id : NumberLong("101"), name : 'scooter', description: 'Small 2-wheel scooter', weight : 3.14, quantity : NumberInt("3") },
    { _id : NumberLong("102"), name : 'car battery', description: '12V car battery', weight : 8.1, quantity : NumberInt("8") },
    { _id : NumberLong("103"), name : '12-pack drill bits', description: '12-pack of drill bits with sizes ranging from #40 to #3', weight : 0.8, quantity : NumberInt("18") },
    { _id : NumberLong("104"), name : 'hammer', description: "12oz carpenter's hammer", weight : 0.75, quantity : NumberInt("4") },
    { _id : NumberLong("105"), name : 'hammer', description: "14oz carpenter's hammer", weight : 0.875, quantity : NumberInt("5") },
    { _id : NumberLong("106"), name : 'hammer', description: "16oz carpenter's hammer", weight : 1.0, quantity : NumberInt("0") },
    { _id : NumberLong("107"), name : 'rocks', description: 'box of assorted rocks', weight : 5.3, quantity : NumberInt("44") },
    { _id : NumberLong("108"), name : 'jacket', description: 'water resistent black wind breaker', weight : 0.1, quantity : NumberInt("2") },
    { _id : NumberLong("109"), name : 'spare tire', description: '24 inch spare tire', weight : 22.2, quantity : NumberInt("5") }
]);

db.customers.insert([
    { _id : NumberLong("1001"), first_name : 'Sally', last_name : 'Thomas', email : 'sally.thomas@acme.com' },
    { _id : NumberLong("1002"), first_name : 'George', last_name : 'Bailey', email : 'gbailey@foobar.com' },
    { _id : NumberLong("1003"), first_name : 'Edward', last_name : 'Walker', email : 'ed@walker.com' },
    { _id : NumberLong("1004"), first_name : 'Anne', last_name : 'Kretchmar', email : 'annek@noanswer.org' }
]);

db.orders.insert([
    { _id : NumberLong("10001"), order_date : new ISODate("2016-01-16T00:00:00Z"), purchaser_id : NumberLong("1001"), quantity : NumberInt("1"), product_id : NumberLong("102") },
    { _id : NumberLong("10002"), order_date : new ISODate("2016-01-17T00:00:00Z"), purchaser_id : NumberLong("1002"), quantity : NumberInt("2"), product_id : NumberLong("105") },
    { _id : NumberLong("10003"), order_date : new ISODate("2016-02-19T00:00:00Z"), purchaser_id : NumberLong("1002"), quantity : NumberInt("2"), product_id : NumberLong("106") },
    { _id : NumberLong("10004"), order_date : new ISODate("2016-02-21T00:00:00Z"), purchaser_id : NumberLong("1003"), quantity : NumberInt("1"), product_id : NumberLong("107") }
]);
