import React, { useState } from "react";

const FileInput = ({ onFileSelect }) => {
  const [selectedFile, setSelectedFile] = useState(null);

  const handleFileChange = (event) => {
    const file = event.target.files[0];
    setSelectedFile(file);
    onFileSelect(file);
  };

  return (
    <div>
      <input type="file" onChange={handleFileChange} />
      {selectedFile && <p>Selected File: {selectedFile.name}</p>}
    </div>
  );
};

export default FileInput;
