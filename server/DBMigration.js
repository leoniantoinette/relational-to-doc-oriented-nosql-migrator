const path = require("path");
const fs = require("fs");
const MySQLDBManager = require("./MySQLDBManager");
const PostgresDBManager = require("./PostgresDBManager");
const LogManager = require("./LogManager");
const WriteToFile = require("./WriteToFile");
const RelationalDatabase = require("./models/RelationalDatabase");
const Table = require("./models/Table");
const NoSQLDatabase = require("./models/NoSQLDatabase");
const Collection = require("./models/Collection");
const Queue = require("./models/Queue");

exports.migrate = async function (dbType, sqlFile, logFile) {
  const sqlFilePath = path.join(__dirname, "uploads", sqlFile.originalname);
  // get log content
  const logFilePath = path.join(__dirname, "uploads", logFile.originalname);
  const logContent = fs.readFileSync(logFilePath, "utf-8");

  switch (dbType) {
    case "mysql":
      // import sql file
      const mySQLDatabaseName = await MySQLDBManager.importSqlFile(sqlFilePath);

      // connect to db, get related information (tables, dll)
      await MySQLDBManager.useDatabase(mySQLDatabaseName);
      var relationalDB = new RelationalDatabase(dbType, mySQLDatabaseName);

      // get list table and its data
      const mySQLTableData = await MySQLDBManager.getTableData(
        mySQLDatabaseName
      );
      for (const row of mySQLTableData) {
        // get num of rows
        const numOfRows = await MySQLDBManager.getTableRows(row.TABLE_NAME);
        const table = new Table(
          row.TABLE_NAME,
          numOfRows,
          row.num_foreign_keys,
          row.reference_status == "Referenced by other tables" ? true : false
        );
        relationalDB.addTable(table);
      }

      // get columns for each table
      const mySQLTableColumns = await MySQLDBManager.getTableColumns(
        mySQLDatabaseName
      );
      mySQLTableColumns.forEach((row) => {
        let table = relationalDB.getTable(row.TABLE_NAME);
        let columns = row.table_columns.split(",").map((item) => item.trim());
        columns.forEach((c) => {
          table.addColumn(c);
        });
      });

      // get referenced and its referencing tables
      const mySQLReferenceInfo = await MySQLDBManager.getReferenceInfo(
        mySQLDatabaseName
      );
      mySQLReferenceInfo.forEach((row) => {
        let table = relationalDB.getTable(row.REFERENCED_TABLE_NAME);
        let referencingTables = row.referencing_tables
          .split(",")
          .map((item) => item.trim());
        referencingTables.forEach((t) => {
          table.addReferencingTable(t);
        });
      });

      // process log (preprocess, parse, and count information workload)
      LogManager.processLog(logContent, relationalDB);

      break;

    case "postgresql":
      // import sql file
      const postgresDatabaseName = await PostgresDBManager.importSqlFile(
        sqlFilePath
      );

      // connect to db, get related information (tables, dll)
      var relationalDB = new RelationalDatabase(dbType, postgresDatabaseName);

      // get list table and its data
      const postgresTableData = await PostgresDBManager.getTableData();
      for (const row of postgresTableData) {
        // get num of rows
        const numOfRows = await PostgresDBManager.getTableRows(row.table_name);
        const table = new Table(
          row.table_name,
          Number(numOfRows),
          Number(row.num_foreign_keys),
          row.reference_status == "Referenced by other tables" ? true : false
        );
        relationalDB.addTable(table);
      }

      // get columns for each table
      const postgresTableColumns = await PostgresDBManager.getTableColumns();
      postgresTableColumns.forEach((row) => {
        let table = relationalDB.getTable(row.table_name);
        let columns = row.table_columns.split(",").map((item) => item.trim());
        columns.forEach((c) => {
          table.addColumn(c);
        });
      });

      // get referenced and its referencing tables
      const postgresReferenceInfo = await PostgresDBManager.getReferenceInfo();
      postgresReferenceInfo.forEach((row) => {
        let table = relationalDB.getTable(row.referenced_table);
        let referencingTables = row.referencing_tables
          .split(",")
          .map((item) => item.trim());
        referencingTables.forEach((t) => {
          table.addReferencingTable(t);
        });
      });

      // process log (preprocess, parse, and count information workload)
      LogManager.processLog(logContent, relationalDB);

      break;
  }

  // schema conversion
  var noSQLDB = await convertSchema(relationalDB);

  // mapping data
  var result = await mappingData(relationalDB.databaseType, noSQLDB);

  const resultFilePath = path.join(__dirname, "results");
  await WriteToFile.writeToJsonArchive(result, resultFilePath);

  const tables = relationalDB.tables;
  const collections = noSQLDB.collections;

  return { tables, collections };
};

async function isSelfReferencing(dbName, dbType, tableName) {
  let foreignKeys;
  if (dbType == "mysql") {
    foreignKeys = await MySQLDBManager.findFK(dbName, tableName);
  } else if (dbType == "postgresql") {
    foreignKeys = await PostgresDBManager.findFK(tableName);
  }

  let referencedTables = foreignKeys.map(
    (row) => row.REFERENCED_TABLE_NAME || row.referenced_table_name
  );
  for (const referencedTable of referencedTables) {
    if (referencedTable === tableName) {
      return true;
    }
  }

  return false;
}

async function createTableQueue(relationalDB, tables) {
  var queue = new Queue();
  var trackTables = [...tables];
  var evaluationQueue = new Queue();

  // initialize queue with tables with 0 foreign key
  tables.forEach((table) => {
    if (table.numOfForeignKeys === 0) {
      queue.enqueue(table);
      evaluationQueue.enqueue(table);
      trackTables = trackTables.filter((t) => t !== table);
    }
  });

  // case all tables have foreign key
  if (queue.isEmpty() && evaluationQueue.isEmpty()) {
    // find self-referencing table with 1 fk
    for (const table of trackTables) {
      if (table.numOfForeignKeys === 1) {
        let isSelfReferencingTable = await isSelfReferencing(
          relationalDB.name,
          relationalDB.databaseType,
          table.name
        );
        if (isSelfReferencingTable) {
          queue.enqueue(table);
          evaluationQueue.enqueue(table);
          trackTables = trackTables.filter((t) => t !== table);
        }
      }
    }
  }

  // evaluate all tables remaining and insert to queue
  while (!evaluationQueue.isEmpty()) {
    let currentTable = evaluationQueue.dequeue();
    if (currentTable.isReferenced) {
      for (const refTableName of currentTable.referencingTables) {
        const refTable = relationalDB.getTable(refTableName);

        // check whether refTable is still not push into queue
        if (trackTables.includes(refTable)) {
          if (refTable.numOfForeignKeys === 1) {
            // refTable only referenced current table
            queue.enqueue(refTable);
            evaluationQueue.enqueue(refTable);
            trackTables = trackTables.filter((t) => t !== refTable);
          } else {
            // check if all referenced table of referencing table have added to queue or is a self-referencing table
            let foreignKeys;
            if (relationalDB.databaseType == "mysql") {
              foreignKeys = await MySQLDBManager.findFK(
                relationalDB.name,
                refTable.name
              );
            } else if (relationalDB.databaseType == "postgresql") {
              foreignKeys = await PostgresDBManager.findFK(refTable.name);
            }

            let referencedTables = foreignKeys.map(
              (row) => row.REFERENCED_TABLE_NAME || row.referenced_table_name
            );
            let isAdded = true;

            referencedTables.forEach((referencedTable) => {
              if (
                !queue.containsTable(referencedTable) &&
                referencedTable !== refTable.name
              ) {
                isAdded = false;
              }
            });

            // if yes, add referencing table to queue
            if (isAdded) {
              queue.enqueue(refTable);
              evaluationQueue.enqueue(refTable);
              trackTables = trackTables.filter((t) => t !== refTable);
            }
          }
        }
      }
    }
  }

  return queue;
}

// embeddedTable di-embed ke collection
async function oneWayEmbedding(
  databaseName,
  databaseType,
  noSQLDatabase,
  embeddedTable
) {
  var foreignKeys;
  if (databaseType == "mysql") {
    var foreignKeys = await MySQLDBManager.findFK(
      databaseName,
      embeddedTable.name
    );
  } else if (databaseType == "postgresql") {
    var foreignKeys = await PostgresDBManager.findFK(embeddedTable.name);
  }

  var fkColumn = foreignKeys[0].COLUMN_NAME || foreignKeys[0].column_name;
  var referencedTable =
    foreignKeys[0].REFERENCED_TABLE_NAME ||
    foreignKeys[0].referenced_table_name;

  var collection = noSQLDatabase.getCollection(referencedTable);

  var attributes = [];
  embeddedTable.columns.forEach((column) => {
    if (column != fkColumn) {
      attributes.push(column);
    }
  });

  let embeddedCollection = new Collection(embeddedTable.name, attributes);
  collection.addEmbeddedCollection(embeddedCollection);
}

// embeddedTable di-embed ke 2 collections
async function twoWayEmbedding(
  databaseName,
  relationalDB,
  noSQLDatabase,
  embeddedTable
) {
  var foreignKeys;
  if (relationalDB.databaseType == "mysql") {
    foreignKeys = await MySQLDBManager.findFK(databaseName, embeddedTable.name);
  } else if (relationalDB.databaseType == "postgresql") {
    foreignKeys = await PostgresDBManager.findFK(embeddedTable.name);
  }

  var fkColumn1 = foreignKeys[0].COLUMN_NAME || foreignKeys[0].column_name;
  var referencedTable1 =
    foreignKeys[0].REFERENCED_TABLE_NAME ||
    foreignKeys[0].referenced_table_name;
  var referencedColumn1 =
    foreignKeys[0].REFERENCED_COLUMN_NAME ||
    foreignKeys[0].referenced_column_name;
  var fkColumn2 = foreignKeys[1].COLUMN_NAME || foreignKeys[1].column_name;
  var referencedTable2 =
    foreignKeys[1].REFERENCED_TABLE_NAME ||
    foreignKeys[1].referenced_table_name;
  var referencedColumn2 =
    foreignKeys[1].REFERENCED_COLUMN_NAME ||
    foreignKeys[1].referenced_column_name;

  var collection1 = noSQLDatabase.getCollection(referencedTable1);
  var collection2 = noSQLDatabase.getCollection(referencedTable2);
  var table1 = relationalDB.getTable(referencedTable1);
  var table2 = relationalDB.getTable(referencedTable2);

  var attributes1 = [];
  var attributes2 = [];
  const filteredColumns = embeddedTable.columns.filter(
    (column) => column !== fkColumn1 && column !== fkColumn2
  );
  attributes1 = attributes1.concat(filteredColumns);
  attributes2 = attributes2.concat(filteredColumns);

  attributes2 = attributes2.concat(
    table1.columns.filter((col) => col !== referencedColumn1)
  );
  attributes1 = attributes1.concat(
    table2.columns.filter((col) => col !== referencedColumn2)
  );

  let embeddedCollection1 = new Collection(embeddedTable.name, attributes1);
  let embeddedCollection2 = new Collection(embeddedTable.name, attributes2);
  embeddedCollection1.addEmbeddedAttributeFrom(collection2.name);
  embeddedCollection2.addEmbeddedAttributeFrom(collection1.name);
  collection1.addEmbeddedCollection(embeddedCollection1);
  collection2.addEmbeddedCollection(embeddedCollection2);
}

async function referencing(
  databaseName,
  databaseType,
  noSQLDatabase,
  referringTable
) {
  var foreignKeys;
  if (databaseType == "mysql") {
    foreignKeys = await MySQLDBManager.findFK(
      databaseName,
      referringTable.name
    );
  } else if (databaseType == "postgresql") {
    foreignKeys = await PostgresDBManager.findFK(referringTable.name);
  }
  var fkColumn = foreignKeys.map((row) => row.COLUMN_NAME || row.column_name);

  var attributes = [];
  referringTable.columns.forEach((column) => {
    if (fkColumn.includes(column)) {
      attributes.push(column + "_REF");
    } else {
      attributes.push(column);
    }
  });

  let collection = new Collection(referringTable.name, attributes);
  noSQLDatabase.addCollection(collection);
}

async function convertSchema(relationalDB) {
  var noSQLDB = new NoSQLDatabase(relationalDB.name);

  var tables = relationalDB.tables;
  var queue = await createTableQueue(relationalDB, tables);

  while (!queue.isEmpty()) {
    let currentTable = queue.dequeue();
    if (currentTable.numOfForeignKeys === 0) {
      // create new collection
      var collection = new Collection(currentTable.name, currentTable.columns);
      noSQLDB.addCollection(collection);
    } else {
      // check if table is a self referencing table
      let isSelfReferencingTable = await isSelfReferencing(
        relationalDB.name,
        relationalDB.databaseType,
        currentTable.name
      );
      if (isSelfReferencingTable) {
        // referencing
        await referencing(
          relationalDB.name,
          relationalDB.databaseType,
          noSQLDB,
          currentTable
        );
      } else {
        if (currentTable.numOfForeignKeys <= 2) {
          let useReferencing = false;
          if (currentTable.uaf > relationalDB.maf) {
            useReferencing = true;
          }
          if (currentTable.isReferenced && !useReferencing) {
            for (let i = 0; i < currentTable.referencingTables.length; i++) {
              const refTable = relationalDB.getTable(
                currentTable.referencingTables[i]
              );
              if (refTable.uaf > relationalDB.maf) {
                useReferencing = true;
                break;
              }
            }
          }

          if (useReferencing) {
            // referencing
            await referencing(
              relationalDB.name,
              relationalDB.databaseType,
              noSQLDB,
              currentTable
            );
          } else {
            if (currentTable.numOfForeignKeys == 2) {
              // two way embedding
              await twoWayEmbedding(
                relationalDB.name,
                relationalDB,
                noSQLDB,
                currentTable
              );
            } else {
              // one way embedding
              await oneWayEmbedding(
                relationalDB.name,
                relationalDB.databaseType,
                noSQLDB,
                currentTable
              );
            }
          }
        } else {
          // referencing
          await referencing(
            relationalDB.name,
            relationalDB.databaseType,
            noSQLDB,
            currentTable
          );
        }
      }
    }
  }

  return noSQLDB;
}

async function mappingData(databaseType, noSQLDB) {
  var output = [];

  for (const collection of noSQLDB.collections) {
    var collectionData = await mapping(
      noSQLDB.name,
      databaseType,
      null,
      null,
      collection
    );
    output.push(collectionData);
  }

  return output;
}

async function mapping(
  databaseName,
  databaseType,
  prevCollectionName,
  prevCollectionData,
  collection
) {
  var output = [];
  var documents = [];
  var datas;
  if (databaseType == "mysql") {
    datas = await MySQLDBManager.getAllDatas(collection.name);
  } else if (databaseType == "postgresql") {
    datas = await PostgresDBManager.getAllDatas(collection.name);
  }

  var isRootBlock = true;
  if (prevCollectionData) {
    isRootBlock = false;

    // get foreign key info
    var foreignKeys;
    if (databaseType == "mysql") {
      var foreignKeys = await MySQLDBManager.findCertainFK(
        databaseName,
        collection.name,
        prevCollectionName
      );
    } else if (databaseType == "postgresql") {
      var foreignKeys = await PostgresDBManager.findCertainFK(
        collection.name,
        prevCollectionName
      );
    }
    var fkColumn = foreignKeys[0].COLUMN_NAME || foreignKeys[0].column_name;
    var referencedColumn =
      foreignKeys[0].REFERENCED_COLUMN_NAME ||
      foreignKeys[0].referenced_column_name;
  }

  // loop for each data
  for (const data of datas) {
    var document = {};

    if (isRootBlock) {
      // map data from collection
      collection.attributes.forEach((attribute) => {
        for (const columnName in data) {
          if (attribute == columnName || attribute == columnName + "_REF") {
            document[attribute] = data[columnName];
          }
        }
      });
    } else {
      // check foreign key
      if (prevCollectionData[referencedColumn] == data[fkColumn]) {
        // map data from collection
        collection.attributes.forEach((attribute) => {
          for (const columnName in data) {
            if (attribute == columnName) {
              document[attribute] = data[columnName];
            }
          }
        });

        // map embedded attribute
        if (collection.hasEmbeddedAttribute()) {
          // find data from collection that matched the foreign key
          documents = await mapEmbeddedAttributes(
            databaseName,
            databaseType,
            collection,
            data,
            document
          );

          // add to output here
          documents.forEach((doc) => {
            // check if the object is empty
            if (!isObjectEmpty(doc)) {
              output.push(doc);
            }
          });
        }
      }
    }

    if (!isObjectEmpty(document) && !collection.hasEmbeddedAttribute()) {
      // check whether the collection has embedded collections
      document = await mapEmbeddedCollections(
        databaseName,
        databaseType,
        collection,
        data,
        document
      );

      // check if the object is empty
      if (!isObjectEmpty(document)) {
        output.push(document);
      }
    }
  }

  return output;
}

async function mapEmbeddedCollections(
  databaseName,
  databaseType,
  collection,
  data,
  document
) {
  if (collection.embeddedCollections.length > 0) {
    for (const embeddedCollection of collection.embeddedCollections) {
      const res = await mapping(
        databaseName,
        databaseType,
        collection.name,
        data,
        embeddedCollection
      );

      if (res.length > 0) {
        document[embeddedCollection.name] = res;
      }
    }
  }
  return document;
}

async function mapEmbeddedAttributes(
  databaseName,
  databaseType,
  collection,
  data,
  document
) {
  if (databaseType == "mysql") {
    var foreignKeys = await MySQLDBManager.findCertainFK(
      databaseName,
      collection.name,
      collection.embeddedAttributesFrom
    );
  } else if (databaseType == "postgresql") {
    var foreignKeys = await PostgresDBManager.findCertainFK(
      collection.name,
      collection.embeddedAttributesFrom
    );
  }
  var fkColumn = foreignKeys[0].COLUMN_NAME || foreignKeys[0].column_name;
  var referencedColumn =
    foreignKeys[0].REFERENCED_COLUMN_NAME ||
    foreignKeys[0].referenced_column_name;

  // find embedded attributes data that matched the fk
  var embeddedDatas;
  if (databaseType == "mysql") {
    embeddedDatas = await MySQLDBManager.getSpecificDatas(
      collection.embeddedAttributesFrom,
      referencedColumn,
      data[fkColumn]
    );
  } else if (databaseType == "postgresql") {
    embeddedDatas = await PostgresDBManager.getSpecificDatas(
      collection.embeddedAttributesFrom,
      referencedColumn,
      data[fkColumn]
    );
  }

  var output = [];

  document = await mapEmbeddedCollections(
    databaseName,
    databaseType,
    collection,
    data,
    document
  );

  for (const embeddedData of embeddedDatas) {
    var doc = { ...document };

    // map data from collection
    collection.attributes.forEach((attribute) => {
      for (const columnName in embeddedData) {
        if (attribute == columnName) {
          doc[attribute] = embeddedData[columnName];
        }
      }
    });

    output.push(doc);
  }

  return output;
}

function isObjectEmpty(obj) {
  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      return false;
    }
  }
  return true;
}
