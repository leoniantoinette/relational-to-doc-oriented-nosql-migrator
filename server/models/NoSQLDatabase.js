const Queue = require("./Queue");

class NoSQLDatabase {
  constructor(name) {
    this.name = name;
    this.collections = [];
  }

  addCollection(collection) {
    this.collections.push(collection);
  }

  getCollection(collectionName) {
    let queue = new Queue([...this.collections]);
    while (!queue.isEmpty()) {
      const currentCollection = queue.dequeue();
      if (currentCollection.name === collectionName) {
        return currentCollection;
      }
      if (
        currentCollection.embeddedCollections &&
        currentCollection.embeddedCollections.length > 0
      ) {
        queue.enqueue(...currentCollection.embeddedCollections);
      }
    }
  }
}

module.exports = NoSQLDatabase;
