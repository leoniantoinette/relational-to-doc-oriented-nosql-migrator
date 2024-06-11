class Table {
  constructor(name, numOfRows, numOfForeignKeys, isReferenced) {
    this.name = name;
    this.numOfRows = numOfRows;
    this.columns = [];
    this.numOfForeignKeys = numOfForeignKeys;
    this.isReferenced = isReferenced;
    this.referencingTables = [];
    this.gaf = 0;
    this.uaf = 0;
  }

  addColumn(columnName) {
    this.columns.push(columnName);
  }

  addReferencingTable(table) {
    this.referencingTables.push(table);
  }

  addGaf() {
    this.gaf += this.numOfRows;
  }

  addUaf() {
    this.uaf += this.numOfRows;
  }
}

module.exports = Table;
