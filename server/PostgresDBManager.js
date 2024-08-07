const { Client } = require("pg");
const fs = require("fs");
const moment = require("moment-timezone");
require("dotenv").config();

class PostgresDBManager {
  constructor() {
    this.config = {
      host: process.env.POSTGRES_DB_HOST,
      user: process.env.POSTGRES_DB_USER,
      password: process.env.POSTGRES_DB_PASSWORD,
      database: process.env.POSTGRES_DB_NAME,
      port: process.env.POSTGRES_DB_PORT,
    };

    this.client = new Client(this.config);

    this.connect();
  }

  async connect(client = this.client) {
    try {
      await client.connect();
      console.log("Connected to PostgreSQL");
    } catch (err) {
      console.error("Error connecting to PostgreSQL:", err);
      throw err;
    }
  }

  async disconnect(client = this.client) {
    try {
      await client.end();
      console.log("Disconnected from PostgreSQL");
    } catch (err) {
      console.error("Error disconnecting from PostgreSQL:", err);
      throw err;
    }
  }

  async importSqlFile(sqlFilePath) {
    try {
      // Drop and recreate the database before importing the SQL file
      const dbName = this.client.connectionParameters.database;
      await this.dropAndRecreateDatabase(dbName);

      // Read the SQL file
      var sql = fs.readFileSync(sqlFilePath, "utf8");

      // Remove DROP, CREATE database, and \c command if exist
      sql = this.removeSpecialCommands(sql);

      // Execute SQL file
      await this.client.query(sql);
      console.log("SQL imported successfully");

      return dbName;
    } catch (error) {
      console.error("Error importing SQL file:", error);
      throw error;
    }
  }

  removeSpecialCommands(sqlContent) {
    const commandsToRemove = [
      { pattern: /DROP\s+DATABASE\s+/i, replacement: "-- DROP DATABASE " },
      {
        pattern: /CREATE\s+DATABASE\s+/i,
        replacement: "-- CREATE DATABASE ",
      },
      { pattern: /\\c\s+\w+/gi, replacement: "" },
    ];

    // Apply each command removal or modification
    for (let command of commandsToRemove) {
      sqlContent = sqlContent.replace(command.pattern, command.replacement);
    }

    return sqlContent;
  }

  async dropAndRecreateDatabase(dbName) {
    try {
      // Disconnect first if connected
      await this.disconnect();

      // Connect to the default 'postgres' database for administrative actions
      const defaultClient = new Client({
        host: process.env.POSTGRES_DB_HOST,
        user: process.env.POSTGRES_DB_USER,
        password: process.env.POSTGRES_DB_PASSWORD,
        database: "postgres",
        port: process.env.POSTGRES_DB_PORT,
      });
      await this.connect(defaultClient);
      console.log("Connected to PostgreSQL postgres");

      // Terminate all active connections to the database
      await defaultClient.query(
        `
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = $1
          AND pid <> pg_backend_pid();
      `,
        [dbName]
      );

      // Drop the existing database
      await defaultClient.query(`DROP DATABASE IF EXISTS ${dbName};`);

      // Create a new database with the same name
      await defaultClient.query(`CREATE DATABASE ${dbName};`);
      console.log(`Database '${dbName}' dropped and recreated successfully`);

      await this.disconnect(defaultClient);

      // Reconnect to the recreated database
      this.client = new Client(this.config);
      await this.connect();
    } catch (err) {
      console.error("Error dropping and recreating database:", err);
      throw err;
    }
  }

  async getTableData() {
    try {
      const query = `
      WITH table_info AS (
        SELECT
            c.oid AS table_oid,
            c.relname AS table_name,
            COUNT(DISTINCT tc.constraint_name) AS num_foreign_keys
        FROM
            pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN information_schema.table_constraints tc
            ON tc.table_name = c.relname
            AND tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = n.nspname
        WHERE
            c.relkind = 'r'
            AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        GROUP BY
            c.oid, c.relname
      )
      SELECT
          ti.table_name,
          ti.num_foreign_keys,
          CASE
              WHEN EXISTS (
                  SELECT 1
                  FROM pg_constraint ref_con
                  WHERE ref_con.confrelid = ti.table_oid
              ) THEN 'Referenced by other tables'
              ELSE 'Not referenced by other tables'
          END AS reference_status
      FROM
          table_info ti
      ORDER BY
          ti.table_name;    
      `;

      const res = await this.client.query(query);
      return res.rows;
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

      const res = await this.client.query(query);
      return res.rows[0].num_rows;
    } catch (error) {
      throw error;
    }
  }

  async getTableColumns() {
    try {
      const query = `
      SELECT
          c.relname AS table_name,
          array_to_string(array_agg(a.attname ORDER BY a.attnum), ', ') AS table_columns
      FROM
          pg_class c
      JOIN
          pg_attribute a ON a.attrelid = c.oid
      JOIN
          pg_namespace n ON n.oid = c.relnamespace
      WHERE
          c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND a.attnum > 0
      GROUP BY
          c.relname;
      `;

      const res = await this.client.query(query);
      return res.rows;
    } catch (error) {
      throw error;
    }
  }

  async getPrimaryKeys(tableName) {
    try {
      const query = `
      SELECT kcu.column_name
      FROM information_schema.table_constraints tco
      JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name
      WHERE tco.constraint_type = 'PRIMARY KEY'
      AND kcu.table_schema = 'public'
      AND kcu.table_name = '${tableName}';
      `;

      const res = await this.client.query(query);
      return res.rows.map((row) => row.column_name);
    } catch (error) {
      throw error;
    }
  }

  async getForeignKeys(tableName) {
    try {
      const query = `
      SELECT kcu.column_name, ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name
      FROM information_schema.key_column_usage kcu
      JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
      JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name
      WHERE tc.constraint_type = 'FOREIGN KEY'
      AND kcu.table_schema = 'public'
      AND kcu.table_name = '${tableName}';
      `;

      const res = await this.client.query(query);
      return res.rows;
    } catch (error) {
      throw error;
    }
  }

  async getReferenceInfo() {
    try {
      const query = `
      SELECT
          ref_table.relname AS referenced_table,
          array_to_string(array_agg(con_table.relname ORDER BY con_table.relname), ', ') AS referencing_tables
      FROM
          pg_constraint c
      JOIN
          pg_class ref_table ON ref_table.oid = c.confrelid
      JOIN
          pg_class con_table ON con_table.oid = c.conrelid
      JOIN
          pg_namespace n ON n.oid = ref_table.relnamespace
      WHERE
          c.contype = 'f'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      GROUP BY
          ref_table.relname;
      `;

      const res = await this.client.query(query);
      return res.rows;
    } catch (error) {
      throw error;
    }
  }

  async getAllDatas(tableName) {
    try {
      const query = `SELECT * FROM ${tableName};`;

      const res = await this.client.query(query);

      // adjust date to system timezone
      const adjustedRows = res.rows.map((row) => {
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
      const query = `SELECT * FROM ${tableName} WHERE ${columnName} = $1;`;

      const res = await this.client.query(query, [columnValue]);

      // adjust date to system timezone
      const adjustedRows = res.rows.map((row) => {
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

module.exports = new PostgresDBManager();
