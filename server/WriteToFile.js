const path = require("path");
const fs = require("fs");
const archiver = require("archiver");

exports.writeToJsonArchive = async function (listOfCollections, directory) {
  // delete all files in directory
  await deleteFiles(directory, false);

  listOfCollections = listOfCollections.filter(
    (collections) => collections.length > 0
  );

  const jsonFiles = listOfCollections.map((collections, index) => {
    return writeToJSON(collections, index + 1, directory);
  });

  await createZip(jsonFiles, directory);

  // delete all json files
  await deleteFiles(directory, true);

  // delete uploaded file after finished
  const uploadFilePath = path.join(__dirname, "uploads");
  await deleteFiles(uploadFilePath, false);
};

function writeToJSON(collection, index, directory) {
  const fileName = `collection${index}.json`;
  const content = JSON.stringify(collection);
  const filePath = path.join(directory, fileName);

  fs.writeFileSync(filePath, content);

  return filePath;
}

function createZip(fileNames, directory) {
  return new Promise((resolve, reject) => {
    const zipFile = fs.createWriteStream(
      path.join(directory, "collections.zip")
    );
    const archive = archiver("zip");

    zipFile.on("close", () => {
      console.log("Zipped successfully");
      resolve();
    });

    archive.on("error", (err) => {
      console.error("Error", err);
      reject(err);
    });

    archive.pipe(zipFile);

    fileNames.forEach((fileName) => {
      archive.file(fileName, { name: path.basename(fileName) });
    });

    archive.finalize();
  });
}

function deleteFiles(directory, isJsonOnly) {
  return new Promise((resolve, reject) => {
    fs.readdir(directory, (err, files) => {
      if (err) {
        console.error("Error", err);
        reject(err);
        return;
      }

      if (isJsonOnly) {
        files = files.filter((file) => path.extname(file) === ".json");
      }

      const unlinkPromises = files.map((file) => {
        const filePath = path.join(directory, file);
        return new Promise((resolve, reject) => {
          fs.unlink(filePath, (err) => {
            if (err) {
              console.error("Error", err);
              reject(err);
            } else {
              resolve();
            }
          });
        });
      });

      Promise.all(unlinkPromises)
        .then(() => {
          console.log("All files deleted successfully");
          resolve();
        })
        .catch((err) => {
          console.error("Error deleting files", err);
          reject(err);
        });
    });
  });
}
