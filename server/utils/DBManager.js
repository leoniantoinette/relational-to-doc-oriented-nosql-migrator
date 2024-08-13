// dbmanager.js
class DBManager {
  constructor() {
    if (new.target === DBManager) {
      throw new Error("Cannot instantiate an abstract class.");
    }
  }

  async connect() {
    throw new Error("Method 'connect()' must be implemented.");
  }

  async disconnect() {
    throw new Error("Method 'disconnect()' must be implemented.");
  }

  async importSqlFile(sqlFilePath) {
    throw new Error("Method 'importSqlFile(sqlFilePath)' must be implemented.");
  }

  async getTableData() {
    throw new Error("Method 'getTableData()' must be implemented.");
  }

  async getTableRows(tableName) {
    throw new Error("Method 'getTableRows(tableName)' must be implemented.");
  }

  async getTableColumns() {
    throw new Error("Method 'getTableColumns()' must be implemented.");
  }

  async getPrimaryKeys(databaseName = null, tableName) {
    throw new Error("Method 'getPrimaryKeys(tableName)' must be implemented.");
  }

  async getForeignKeys(databaseName = null, tableName) {
    throw new Error("Method 'getForeignKeys(tableName)' must be implemented.");
  }

  async getReferenceInfo() {
    throw new Error("Method 'getReferenceInfo()' must be implemented.");
  } // dbname

  async getAllDatas(tableName) {
    throw new Error("Method 'getAllDatas(tableName)' must be implemented.");
  }

  async getSpecificDatas(tableName, columnName, columnValue) {
    throw new Error(
      "Method 'getSpecificDatas(tableName, columnName, columnValue)' must be implemented."
    );
  }

  isDateField(value) {
    throw new Error("Method 'isDateField(value)' must be implemented.");
  }
}

module.exports = DBManager;
