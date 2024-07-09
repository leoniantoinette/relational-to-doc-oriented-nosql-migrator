import { Box, Text } from "@chakra-ui/react";

const transformCollection = (collections) => {
  var output = [];

  collections.forEach((collection) => {
    const document = transform(collection);
    output.push(document);
  });

  return output;
};

const transform = (collection) => {
  const document = {};

  collection.attributes.forEach((attribute) => {
    document[attribute] = "";
  });

  if (collection.embeddedCollections.length > 0) {
    collection.embeddedCollections.forEach((embeddedCollection) => {
      document[embeddedCollection.name] = [transform(embeddedCollection)];
    });
  }

  return document;
};

const NoSqlCollection = ({ collection: collections }) => {
  const data = transformCollection(collections);

  return (
    <Box>
      {data.map((item, index) => (
        <div key={index}>
          <Text whiteSpace="pre-wrap" py={2}>
            {JSON.stringify(item, null, 4)}
          </Text>
          {index !== data.length - 1 && <hr />}
        </div>
      ))}
    </Box>
  );
};

export default NoSqlCollection;
