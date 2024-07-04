const express = require("express");
const path = require("path");
const multer = require("multer");
const cors = require("cors");
const DBMigration = require("./DBMigration");

const app = express();
const PORT = process.env.PORT || 3001;
require("dotenv").config();

app.use(express.json());

const corsOptions = {
  origin: "*",
  credentials: true,
  optionSuccessStatus: 200,
};

app.use(cors(corsOptions));

// Multer configuration
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "uploads/");
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname);
  },
});

const upload = multer({ storage: storage });

app.post(
  "/migration",
  upload.fields([
    { name: "sqlFile", maxCount: 1 },
    { name: "logFile", maxCount: 1 },
  ]),
  async (req, res) => {
    const dbType = req.body.dbType;
    const sqlFile = req.files["sqlFile"][0];
    const logFile = req.files["logFile"][0];

    try {
      const { tables, collections } = await DBMigration.migrate(
        dbType,
        sqlFile,
        logFile
      );
      collections.forEach((collection) => {
        collection.embeddedCollections.forEach((element) => {});
      });
      res.status(200).json({
        tables: tables,
        collections: collections,
      });
    } catch (error) {
      console.error("Error migrating:", error);
      res.status(500).send("Internal Server Error");
    }
  }
);

app.get("/download/result", (req, res) => {
  const zipFilePath = path.join(__dirname, "results", "collections.zip");
  res.download(zipFilePath);
});

app.listen(PORT, () => {
  console.log("Server is connected");
});
