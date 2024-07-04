const mysql = require("mysql");
const path = require("path");
const fs = require("fs");
const Importer = require("mysql-import");
const moment = require("moment-timezone");
require("dotenv").config();

class MySQLDBManager {
  constructor() {
    this.conn = mysql.createConnection({
      host: process.env.MYSQL_DB_HOST,
      user: process.env.MYSQL_DB_USER,
      password: process.env.MYSQL_DB_PASSWORD,
      port: process.env.MYSQL_DB_PORT,
    });

    this.conn.connect((err) => {
      if (err) {
        console.error("Mysql Database connection error:", err);
      } else {
        console.log("Connected to Mysql Database");
      }
    });
  }

  async importSqlFile(sqlFilePath) {
    try {
      // validate the sql file
      const dbName = await this.validateSqlFile(sqlFilePath);

      this.importer = new Importer({
        host: process.env.MYSQL_DB_HOST,
        user: process.env.MYSQL_DB_USER,
        password: process.env.MYSQL_DB_PASSWORD,
      });
      console.log("Connected with importer");

      // start the import process
      await this.importer.import(sqlFilePath);

      return dbName;
    } catch (error) {
      console.error("Error importing SQL file:", error);
      throw error;
    }
  }

  useDatabase(databaseName) {
    return new Promise((resolve, reject) => {
      this.conn.query(`USE ${databaseName}`, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  runQuery(query) {
    return new Promise((resolve, reject) => {
      this.conn.query(query, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  async validateSqlFile(sqlFilePath) {
    const sqlFileName = path.basename(sqlFilePath, ".sql");
    const content = fs.readFileSync(sqlFilePath, "utf8");

    const hasDropDatabase = /^DROP\s+DATABASE\s+/i.test(content);

    const dbNameMatch = /USE\s+(\w+)\s*;/i.exec(content);
    let dbName = dbNameMatch ? dbNameMatch[1] : null;
    if (!dbName) {
      dbName = sqlFileName;
    }

    if (!hasDropDatabase) {
      await this.runQuery(`DROP DATABASE IF EXISTS ${dbName};`);
    }

    return dbName;
  }

  async getTableData(databaseName) {
    try {
      const query = `
      SELECT t.TABLE_NAME, COUNT(k.REFERENCED_TABLE_NAME) AS num_foreign_keys,
        CASE
          WHEN EXISTS (SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = t.TABLE_SCHEMA AND REFERENCED_TABLE_NAME = t.TABLE_NAME) 
          THEN 'Referenced by other tables' 
          ELSE 'Not referenced by other tables' 
        END AS reference_status 
      FROM INFORMATION_SCHEMA.TABLES t 
      LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k 
        ON k.TABLE_SCHEMA = t.TABLE_SCHEMA AND k.TABLE_NAME = t.TABLE_NAME AND k.REFERENCED_TABLE_NAME IS NOT NULL 
      WHERE t.TABLE_SCHEMA = '${databaseName}' AND t.TABLE_TYPE = 'BASE TABLE' 
      GROUP BY t.TABLE_NAME;
      `;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      return rows.map((row) => ({ ...row }));
    } catch (error) {
      throw error;
    }
  }

  async getTableRows(tableName) {
    try {
      const query = `
      SELECT COUNT(*) AS num_rows
      FROM ${tableName};
      `;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      return rows[0].num_rows;
    } catch (error) {
      throw error;
    }
  }

  async getTableColumns(databaseName) {
    try {
      const query = `
      SELECT TABLE_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION SEPARATOR ', ') AS table_columns
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${databaseName}'
      GROUP BY TABLE_NAME;
      `;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      return rows.map((row) => ({ ...row }));
    } catch (error) {
      throw error;
    }
  }

  async getReferenceInfo(databaseName) {
    try {
      const query = `
      SELECT REFERENCED_TABLE_NAME, GROUP_CONCAT(DISTINCT TABLE_NAME ORDER BY TABLE_NAME ASC SEPARATOR ', ') AS referencing_tables 
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
      WHERE TABLE_SCHEMA = '${databaseName}' AND REFERENCED_TABLE_NAME IS NOT NULL 
      GROUP BY REFERENCED_TABLE_NAME;
      `;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      return rows.map((row) => ({ ...row }));
    } catch (error) {
      throw error;
    }
  }

  async findFK(databaseName, tableName) {
    try {
      const query = `
      SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
      WHERE
        TABLE_SCHEMA = '${databaseName}'
        AND TABLE_NAME = '${tableName}'
        AND REFERENCED_TABLE_NAME IS NOT NULL;
      `;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      return rows.map((row) => ({ ...row }));
    } catch (error) {
      throw error;
    }
  }

  async findCertainFK(databaseName, tableName, referencedTablename) {
    try {
      const query = `
      SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
      WHERE
        TABLE_SCHEMA = '${databaseName}'
        AND TABLE_NAME = '${tableName}'
        AND REFERENCED_TABLE_NAME = '${referencedTablename}';
      `;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      return rows.map((row) => ({ ...row }));
    } catch (error) {
      throw error;
    }
  }

  async getAllDatas(tableName) {
    try {
      const query = `SELECT * FROM ${tableName};`;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      // adjust date to system timezone
      const adjustedRows = rows.map((row) => {
        Object.keys(row).forEach((key) => {
          if (this.isDateField(row[key])) {
            row[key] = moment(row[key]).tz("Asia/Bangkok").format("YYYY-MM-DD");
          }
        });
        return { ...row };
      });

      return adjustedRows;
    } catch (error) {
      throw error;
    }
  }

  async getSpecificDatas(tableName, columnName, columnValue) {
    try {
      const query = `SELECT * FROM ${tableName} WHERE ${columnName} = "${columnValue}";`;

      const rows = await new Promise((resolve, reject) => {
        this.conn.query(query, (err, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        });
      });

      // adjust date to system timezone
      const adjustedRows = rows.map((row) => {
        Object.keys(row).forEach((key) => {
          if (this.isDateField(row[key])) {
            row[key] = moment(row[key]).tz("Asia/Bangkok").format("YYYY-MM-DD");
          }
        });
        return { ...row };
      });

      return adjustedRows;
    } catch (error) {
      throw error;
    }
  }

  isDateField(value) {
    return (
      value instanceof Date ||
      (typeof value === "string" && !isNaN(Date.parse(value)))
    );
  }
}

module.exports = new MySQLDBManager();
