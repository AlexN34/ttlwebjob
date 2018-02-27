'use strict';
if (process.argv.length < 4){
  console.log("Incorrect number of arguments: " + JSON.stringify(process.argv));
  process.exit(1);
}
var DocumentDBClient = require('documentdb').DocumentClient
, config = require('./private/config')
, databaseId = config.databaseId
, collectionId = config.collectionId
, dbLink
, internalIdToChange = process.argv[2]
, ttlToAdd = process.argv[3]
, collLink;

var host = config.host;
var masterKey = config.masterKey;

var client = new DocumentDBClient( host, { masterKey: masterKey });

var newDoc = {
  "internal_id": internalIdToChange,
  "ttl": Number(ttlToAdd)
};

init(function (err) {
  if (!err) {
    dbLink = 'dbs/' + databaseId;
    collLink = dbLink + '/colls/' + collectionId;
    console.log("Getting document");
    getDocument(internalIdToChange, collLink, function(doc) {
      console.log(JSON.stringify(doc));
      if (doc) {
        console.log("About to try replace: " +JSON.stringify(doc));
        //existing record for this internal_id - replace it with new ttl
        newDoc['id'] = doc['id'];
        client.replaceDocument(doc['_self'], newDoc, function(err, replaced) {
          if (err) {
            console.log("Error in replacing document");
            console.log(err);
          } else {
            console.log("Replaced ttl record:" + JSON.stringify(replaced));
          }
        });

      } else {
        // no current record for this internal_id
        console.log("About to try create: " +JSON.stringify(newDoc));
        client.createDocument(collLink, newDoc, function (err, created) {
          if (err) {
            console.log("Error in creating document");
            console.log(err);
          } else {
            console.log("Created ttl record:" + JSON.stringify(created));
          }
        });
      }
    });

  }
});

function init(callback) {
  getOrCreateDatabase(databaseId, function (db) {
    getOrCreateCollection(db._self, collectionId, function (coll) {
      callback();
    });
  });
}



function getDocument(internalId, collLink, callback) {
  var querySpec = {
    query: 'SELECT * FROM root r WHERE r.internal_id=@id',
    parameters: [
      {
        name: '@id',
        value: internalId
      }
    ]
  };

  var queryIterator = client.queryDocuments(collLink, querySpec);
  queryIterator.toArray(function(err, docs) {
    if (err) {
      if (err.code === 429) {
      } else {
        handleError(err);
      }
    } else if (docs.length >= 0) {
      if (docs.length > 1) {
        throw Error("Multiple records for this ID in ttl collection");
      } else {callback(docs[0]);}
    } else {
      callback(null);
    }
  });
}


function getOrCreateCollection(dbLink, id, callback) {

  var querySpec = {
    query: 'SELECT * FROM root r WHERE r.id=@id',
    parameters: [
      {
        name: '@id',
        value: id
      }
    ]
  };

  client.queryCollections(dbLink, querySpec).toArray(function (err, results) {
    if (err) {
      handleError(err);

      //collection not found, create it
    } else if (results.length === 0) {
      var collDef = { id: id };

      client.createCollection(dbLink, collDef, function (err, created) {
        if (err) {
          handleError(err);

        } else {
          callback(created);
        }
      });

      //collection found, return it
    } else {
      callback(results[0]);
    }
  });
}

function getOrCreateDatabase(id, callback) {
  //we're using queryDatabases here and not readDatabase
  //readDatabase will throw an exception if resource is not found
  //queryDatabases will not, it will return empty resultset.

  var querySpec = {
    query: 'SELECT * FROM root r WHERE r.id=@id',
    parameters: [
      {
        name: '@id',
        value: id
      }
    ]
  };

  client.queryDatabases(querySpec).toArray(function (err, results) {
    if (err)
      handleError(err);
    else {
        callback(results[0]);
    }
  });
}

function handleError(error) {
  console.log('\nAn error with code \'' + error.code + '\' has occurred:');
  console.log('\t' + JSON.parse(error.body).message);
}
