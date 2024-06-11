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
              {item.columns.map((column, columnIndex) => (
                <Flex key={columnIndex}>
                  <Text>{column}</Text>
                </Flex>
              ))}
            </Box>
          </Flex>

          {index !== data.length - 1 && <hr />}
        </div>
      ))}
    </Box>
  );
};

export default RelationalTable;
