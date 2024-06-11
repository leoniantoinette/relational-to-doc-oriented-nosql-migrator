class Collection {
  constructor(name, attributes) {
    this.name = name;
    this.attributes = attributes;
    this.embeddedCollections = [];
    this.embeddedAttributesFrom = "";
  }

  addEmbeddedCollection(embeddedCollection) {
    this.embeddedCollections.push(embeddedCollection);
  }

  addEmbeddedAttributeFrom(collection) {
    this.embeddedAttributesFrom = collection;
  }

  hasEmbeddedAttribute() {
    return this.embeddedAttributesFrom !== "";
  }
}

module.exports = Collection;
