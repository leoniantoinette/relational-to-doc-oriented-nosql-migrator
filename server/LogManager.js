const { Parser } = require("node-sql-parser");
const parser = new Parser();

// const opt = {
//   database: "MySQL",
// };

exports.processLog = function (logContent, database) {
  const queries = preProcessLog(logContent, database.databaseType);
  parseLog(queries, database);
};

function preProcessLog(logContent, databaseType) {
  var result = [];

  switch (databaseType) {
    case "mysql":
      // yymmdd hh:mm:ss thread_id command_type query_body
      const mysqlLogPattern =
        /(?:\d{6}\s*\d{1,2}:\d{2}:\d{2}\s*)?\d+\s*(Connect|Quit|Query|Init\sDB|Sleep|Shutdown|Create\sDB|Drop\sDB|Refresh|Statistics|Processlist|Kill|Change\suser|Binlog\sDump|Table\sDump|Field\sList|Execute|Prepare|Close\sstmt|Reset\sstmt|Fetch|Daemon)\s*([\s\S]+?)(?=(?:\d{6}\s*\d{1,2}:\d{2}:\d{2}\s*)?\d+\s*(?:Connect|Quit|Query|Init\sDB|Sleep|Shutdown|Create\sDB|Drop\sDB|Refresh|Statistics|Processlist|Kill|Change\suser|Binlog\sDump|Table\sDump|Field\sList|Execute|Prepare|Close\sstmt|Reset\sstmt|Fetch|Daemon)|$)/gs;

      var match;
      while ((match = mysqlLogPattern.exec(logContent)) !== null) {
        const commandType = match[1];
        var queryBody = match[2];

        if (commandType == "Query") {
          queryBody = queryBody.replace(/\s+/g, " ").trim();
          if (/^(select|insert|update|delete|create)\b/i.test(queryBody)) {
            result.push(queryBody);
          }
        }
      }

      break;

    case "postgresql":
      // yyyy-mm-dd hh:mm:ss.sss +zz [process_id] log_level: query_body
      // zz : timezone
      const postgresLogPattern =
        /(?:\d{4}-\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2}\.\d{3}\s*\+\d{2}\s*\[\d+\]\s*(?:LOG:\s*statement:|STATEMENT:)\s*)([\s\S]+?)(?=\d{4}-\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2}\.\d{3}\s*\+\d{2}|$)/gs;

      var match;
      while ((match = postgresLogPattern.exec(logContent)) !== null) {
        var queryBody = match[1].replace(/\s+/g, " ").trim();
        if (/^(select|insert|update|delete|create)\b/i.test(queryBody)) {
          result.push(queryBody);
        }
      }

      break;
  }

  return result;
}

function parseLog(queries, database) {
  var opt;
  if (database.databaseType == "mysql") {
    opt = {
      database: "MySQL",
    };
  } else if (database.databaseType == "postgresql") {
    opt = {
      database: "Postgresql",
    };
  }

  queries.forEach((query) => {
    console.log("query", query);
    const tableList = parser.tableList(query, opt);
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
