db.createCollection("orders");
db.createCollection("users");

// Insert into 'orders' collection
db.orders.insertMany([
    {
        id: 1,
        name: "pizza",
        description: "pizza with peperoni"
    },
    {
        id: 2,
        name: "kebab",
        description: "kebab with mayonnaise"
    }
]);