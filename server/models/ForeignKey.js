class ForeignKey {
  constructor(columnName, referencedTableName, referencedColumnName) {
    this.columnName = columnName;
    this.referencedTableName = referencedTableName;
    this.referencedColumnName = referencedColumnName;
  }
}

module.exports = ForeignKey;
