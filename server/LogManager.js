const { Parser } = require("node-sql-parser");
const parser = new Parser();

const opt = {
  database: "MySQL",
};

exports.processLog = function (logContent, database) {
  const queries = preProcessLog(logContent);
  parseLog(queries, database);
};

function preProcessLog(logContent) {
  // yymmdd hh:mm:ss thread_id command_type query_body
  const logPattern =
    /\d{6}\s*\d{1,2}:\d{2}:\d{2}\s*\d+\s*(\w+)\s*(.+?)(?=\d{6}\s*\d{1,2}:\d{2}:\d{2}\s*\d+\s*\w+|$)/gs;
  var result = [];

  var match;
  while ((match = logPattern.exec(logContent)) !== null) {
    const commandType = match[1];
    var queryBody = match[2];

    if (commandType == "Query") {
      queryBody = queryBody.replace(/\s+/g, " ").trim();
      result.push(queryBody);
    }
  }

  return result;
}

function parseLog(queries, database) {
  queries.forEach((query) => {
    const tableList = parser.tableList(query);
    countInfoWorkload(tableList, database);
  });

  database.calculateMaf();
}

function countInfoWorkload(tableList, database) {
  // format: {type}::{dbName}::{tableName}
  tableList.forEach((t) => {
    const datas = t.split("::");
    const type = datas[0];
    const tablename = datas[2];

    // check if the type is select, insert, update, or delete (not create etc)
    if (
      type == "select" ||
      type == "insert" ||
      type == "update" ||
      type == "delete"
    ) {
      let table = database.getTable(tablename);
      if (table) {
        table.addGaf();
        if (type != "select") {
          table.addUaf();
        }
      }
    }
  });
}
