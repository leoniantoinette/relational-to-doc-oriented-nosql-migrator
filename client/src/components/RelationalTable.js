import React from "react";
import { Box, Flex, Text } from "@chakra-ui/react";

const RelationalTable = ({ data }) => {
  return (
    <Box>
      <Flex mb={2}>
        <Box flex={1}>
          <Text fontWeight="bold" align="center">
            Table
          </Text>
        </Box>

        <Box flex={1} pl={2}>
          <Text fontWeight="bold" align="center">
            Columns
          </Text>
        </Box>
      </Flex>

      <hr />

      {data.map((item, index) => (
        <div key={index}>
          <Flex key={index} mb={4} mt={2}>
            <Box flex={1}>
              <Text fontWeight="bold">{item.name}</Text>
            </Box>

            <Box flex={1} pl={2}>
              {item.columns.map((column, columnIndex) => {
                // check primary key
                const isPrimaryKey = item.primaryKeys.includes(column);
                // check foreign key
                const foreignKey = item.foreignKeys.find(
                  (fk) => fk.column_name === column
                );
                const foreignKeyLabel = foreignKey
                  ? `(FK to ${foreignKey.referenced_table_name}.${foreignKey.referenced_column_name})`
                  : "";

                return (
                  <Flex key={columnIndex} mb={1}>
                    <Text>
                      {column}
                      {isPrimaryKey && " (PK)"}
                      {foreignKeyLabel && ` ${foreignKeyLabel}`}
                    </Text>
                  </Flex>
                );
              })}
            </Box>
          </Flex>

          {index !== data.length - 1 && <hr />}
        </div>
      ))}
    </Box>
  );
};

export default RelationalTable;
