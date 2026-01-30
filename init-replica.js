// Attendre que l'initiation soit complète
var config = {
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo1:27017", priority: 2 },
    { _id: 1, host: "mongo2:27017", priority: 1 },
    { _id: 2, host: "mongo3:27017", priority: 1 }
  ]
};

rs.initiate(config);

// Attendre que le replica set soit prêt
sleep(5000);

// Vérifier le statut
print("Status du replica set:");
printjson(rs.status());