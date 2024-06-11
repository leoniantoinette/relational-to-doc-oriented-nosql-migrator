import "./App.css";
import { useEffect, useState } from "react";
import {
  Box,
  Button,
  Heading,
  Center,
  ChakraProvider,
  Container,
  FormControl,
  FormLabel,
  Input,
  Text,
  Flex,
  FormHelperText,
} from "@chakra-ui/react";
import { DownloadIcon } from "@chakra-ui/icons";
import axios from "axios";
import RelationalTable from "./components/RelationalTable";
import NoSqlCollection from "./components/NoSqlCollection";

function App() {
  const [sqlFile, setSqlFile] = useState(null);
  const [logFile, setLogFile] = useState(null);
  const [relationalTables, setRelationalTables] = useState([]);
  const [noSqlCollections, setNoSqlCollections] = useState([]);
  const [showResult, setShowResult] = useState(false);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (showResult) {
      // scroll to the result
      const migrationResult = document.getElementById("migration-result");
      if (migrationResult) {
        migrationResult.scrollIntoView({ behavior: "smooth" });
      } else {
        console.log("Migration result element not found");
      }
    }
  }, [showResult]);

  const submitMigration = async (event) => {
    event.preventDefault();
    setShowResult(false);

    if (!sqlFile || !logFile) {
      alert("Please upload all files");
      return;
    }

    setLoading(true);

    const formData = new FormData();
    formData.append("sqlFile", sqlFile);
    formData.append("logFile", logFile);

    try {
      await axios
        .post("http://localhost:3001/migration", formData, {
          headers: {
            "Content-Type": "multipart/form-data",
          },
        })
        .then((response) => {
          setRelationalTables(response.data.tables);
          setNoSqlCollections(response.data.collections);
          setShowResult(true);
        });
    } catch (error) {
      console.error(error);
      alert("Migration failed. Please try again.");
    } finally {
      setLoading(false);
      setSqlFile(null);
      setLogFile(null);
      formData.forEach((_, key) => formData.delete(key));
    }
  };

  const handleSqlFileSelect = (event) => {
    const sqlFile = event.target.files[0];
    setSqlFile(sqlFile);
    console.log("Selected SQL file:", sqlFile);
  };

  const handleLogFileSelect = (event) => {
    const logFile = event.target.files[0];
    setLogFile(logFile);
    console.log("Selected Log file:", logFile);
  };

  const handleDownload = async () => {
    try {
      const response = await axios.get(
        "http://localhost:3001/download/result",
        {
          responseType: "blob",
        }
      );

      const url = window.URL.createObjectURL(new Blob([response.data]));

      const link = document.createElement("a");
      link.href = url;
      link.setAttribute("download", "collections.zip");
      document.body.appendChild(link);
      link.click();

      // remove the temporary URL and link element
      window.URL.revokeObjectURL(url);
      document.body.removeChild(link);
    } catch (error) {
      console.error("Error downloading file:", error);
      alert("Download migration result failed");
    }
  };

  return (
    <ChakraProvider>
      <Container maxW="90%">
        <Heading textAlign="center" as="h1" size="lg" mt={10}>
          Relational to Document-Oriented NoSQL Database Migrator
        </Heading>

        <Center>
          <Box mt={10} p={8} borderWidth="1px" borderRadius="lg" boxShadow="lg">
            <FormControl isRequired>
              <FormLabel fontSize="lg" htmlFor="sqlFile">
                SQL File
              </FormLabel>
              <FormHelperText mb={2}>
                Select your SQL dump file (.sql)
              </FormHelperText>
              <Input
                type="file"
                name="sqlFile"
                accept=".sql"
                value={sqlFile ? undefined : ""}
                onChange={handleSqlFileSelect}
              />
            </FormControl>

            <FormControl mt={6} isRequired>
              <FormLabel fontSize="lg" htmlFor="logFile">
                Log File
              </FormLabel>
              <FormHelperText mb={2}>
                Select your log file (.log)
              </FormHelperText>
              <Input
                type="file"
                name="logFile"
                accept=".log"
                value={logFile ? undefined : ""}
                onChange={handleLogFileSelect}
              />
            </FormControl>

            <Flex justify="center" mt={4}>
              <Button
                fontSize="lg"
                alignSelf="center"
                mt={4}
                colorScheme="blue"
                onClick={submitMigration}
                isLoading={loading}
                loadingText="Migrating"
              >
                Migrate
              </Button>
            </Flex>
          </Box>
        </Center>

        {showResult && (
          <Box
            minH="100vh"
            id="migration-result"
            bg="gray.100"
            py={8}
            borderRadius="lg"
            mt={10}
            display="flex"
            flexDirection="column"
          >
            <Text fontSize="2xl" fontWeight="bold" mb={4} textAlign="center">
              Migration Result
            </Text>

            <Flex mx="auto" px={4} flex="1" width="100%">
              <Flex
                direction="column"
                alignItems="center"
                flex="1"
                mb={{ base: 4, md: 0 }}
                height="calc(100vh - 200px)"
                mr={1}
              >
                <Text
                  fontSize="lg"
                  fontWeight="medium"
                  textAlign="center"
                  mb={2}
                >
                  Relational Database
                </Text>
                <Box
                  bg="white"
                  borderRadius="md"
                  p={4}
                  overflowY="auto"
                  width="100%"
                >
                  <RelationalTable data={relationalTables} />
                </Box>
              </Flex>

              <Flex
                direction="column"
                alignItems="center"
                flex="1"
                mb={{ base: 4, md: 0 }}
                height="calc(100vh - 200px)"
                ml={1}
              >
                <Text
                  fontSize="lg"
                  fontWeight="medium"
                  textAlign="center"
                  mb={2}
                >
                  NoSQL Database
                </Text>
                <Box
                  bg="white"
                  borderRadius="md"
                  p={4}
                  overflowY="auto"
                  width="100%"
                >
                  <NoSqlCollection collection={noSqlCollections} />
                </Box>
              </Flex>
            </Flex>

            <Center>
              <Button
                colorScheme="green"
                size="lg"
                mt={6}
                onClick={handleDownload}
                boxShadow="lg"
              >
                <Box as="span" mr={2} display="inline-flex">
                  <DownloadIcon />
                </Box>
                Download Migration Result
              </Button>
            </Center>
          </Box>
        )}
      </Container>
    </ChakraProvider>
  );
}

export default App;