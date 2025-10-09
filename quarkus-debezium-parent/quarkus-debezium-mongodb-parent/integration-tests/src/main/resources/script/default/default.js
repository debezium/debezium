// 2. Create and populate the products collection
db.products.insertMany([
    {
        id: 1,
        name: "t-shirt",
        description: "red hat t-shirt"
    },
    {
        id: 2,
        name: "sweatshirt",
        description: "blue ibm sweatshirt"
    }
]);

// 3. Create and populate the users collection
db.users.insertMany([
    { name: "alvar" },
    { name: "anisha" },
    { name: "chris" },
    { name: "indra" },
    { name: "jiri" },
    { name: "giovanni" },
    { name: "mario" },
    { name: "rené" },
    { name: "Vojtěch" }
]);
