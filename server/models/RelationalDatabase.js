class RelationalDatabase {
  constructor(databaseType, name) {
    this.databaseType = databaseType;
    this.name = name;
    this.tables = [];
    this.maf = 0;
  }

  addTable(table) {
    this.tables.push(table);
  }

  getTable(tableName) {
    return this.tables.find((table) => table.name === tableName);
  }

  calculateMaf() {
    // calculate total gaf
    let totalGaf = 0;
    this.tables.forEach((table) => {
      totalGaf += table.gaf;
    });
    this.maf = totalGaf * 0.0125;
  }
}

module.exports = RelationalDatabase;
