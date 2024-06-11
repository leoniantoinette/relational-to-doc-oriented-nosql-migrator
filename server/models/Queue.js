// modified queue based on Table model
class Queue {
  constructor(list = []) {
    this.list = list;
  }

  enqueue(item) {
    this.list.push(item);
  }

  enqueueTable(table) {
    if (this.isEmpty()) {
      this.list.push(table);
    } else {
      let isAdded = false;
      for (let i = 0; i < this.list.length; i++) {
        if (table.numOfForeignKeys < this.list[i].numOfForeignKeys) {
          this.list.splice(i, 0, table);
          isAdded = true;
          break;
        }
      }
      if (!isAdded) {
        this.list.push(table);
      }
    }
  }

  dequeue() {
    return this.isEmpty ? this.list.shift() : "Queue is empty";
  }

  isEmpty() {
    return this.list.length === 0;
  }

  contains(element) {
    return this.list.includes(element);
  }

  containsTable(tableName) {
    return this.list.some((table) => table.name === tableName);
  }
}

module.exports = Queue;
