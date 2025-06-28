using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace BasicSQL.Storage
{
    /// <summary>
    /// Scalable file-based storage manager that can handle large datasets (100GB+)
    /// by storing rows in separate data files and keeping only metadata in memory
    /// </summary>
    public class ScalableFileStorageManager
    {
        private readonly string _dataDirectory;
        private readonly string _indexDirectory;
        private readonly string _metaDirectory;
        private readonly int _rowsPerFile;
        private readonly int _bufferSize;

        public ScalableFileStorageManager(string baseDirectory = "data", int rowsPerFile = 10000, int bufferSize = 1024 * 1024)
        {
            _dataDirectory = Path.Combine(baseDirectory, "tables");
            _indexDirectory = Path.Combine(baseDirectory, "indexes");
            _metaDirectory = Path.Combine(baseDirectory, "metadata");
            _rowsPerFile = rowsPerFile;
            _bufferSize = bufferSize;
            
            // Ensure directories exist
            Directory.CreateDirectory(_dataDirectory);
            Directory.CreateDirectory(_indexDirectory);
            Directory.CreateDirectory(_metaDirectory);
        }

        /// <summary>
        /// Gets the metadata file path for a table
        /// </summary>
        public string GetTableMetaFilePath(string tableName)
        {
            return Path.Combine(_metaDirectory, $"{tableName}_meta.json");
        }

        /// <summary>
        /// Gets the data file path for a table and file index
        /// </summary>
        public string GetTableDataFilePath(string tableName, int fileIndex)
        {
            return Path.Combine(_dataDirectory, $"{tableName}_data_{fileIndex:D6}.jsonl");
        }

        /// <summary>
        /// Gets the index file path
        /// </summary>
        public string GetIndexFilePath(string tableName, string columnName)
        {
            return Path.Combine(_indexDirectory, $"{tableName}_{columnName}.json");
        }

        /// <summary>
        /// Loads table metadata from file
        /// </summary>
        public ScalableTableMetadata? LoadTableMetadata(string tableName)
        {
            var filePath = GetTableMetaFilePath(tableName);
            if (!File.Exists(filePath))
                return null;

            try
            {
                var json = File.ReadAllText(filePath);
                return JsonConvert.DeserializeObject<ScalableTableMetadata>(json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to load table metadata '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Saves table metadata to file
        /// </summary>
        public void SaveTableMetadata(string tableName, ScalableTableMetadata metadata)
        {
            var filePath = GetTableMetaFilePath(tableName);
            try
            {
                var json = JsonConvert.SerializeObject(metadata, Formatting.Indented);
                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to save table metadata '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Appends a row to the appropriate data file
        /// </summary>
        public int AppendRow(string tableName, Dictionary<string, object?> row, ScalableTableMetadata metadata)
        {
            var fileIndex = metadata.TotalRows / _rowsPerFile;
            var filePath = GetTableDataFilePath(tableName, fileIndex);
            
            try
            {
                var json = JsonConvert.SerializeObject(row, Formatting.None);
                File.AppendAllText(filePath, json + Environment.NewLine);
                
                var rowId = metadata.TotalRows;
                metadata.TotalRows++;
                metadata.LastModified = DateTime.UtcNow;
                
                SaveTableMetadata(tableName, metadata);
                return rowId;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to append row to table '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Reads rows from data files with pagination support
        /// </summary>
        public IEnumerable<(int rowId, Dictionary<string, object?>)> ReadRows(
            string tableName, 
            ScalableTableMetadata metadata,
            int skip = 0, 
            int take = int.MaxValue)
        {
            var rowsRead = 0;
            var currentRowId = 0;

            var startFileIndex = skip / _rowsPerFile;
            var endFileIndex = Math.Min((skip + take - 1) / _rowsPerFile, (metadata.TotalRows - 1) / _rowsPerFile);

            for (var fileIndex = startFileIndex; fileIndex <= endFileIndex && rowsRead < take; fileIndex++)
            {
                var filePath = GetTableDataFilePath(tableName, fileIndex);
                if (!File.Exists(filePath))
                    continue;

                var startRowInFile = fileIndex * _rowsPerFile;
                var skipInFile = Math.Max(0, skip - startRowInFile);

                using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                using var reader = new StreamReader(fileStream, bufferSize: _bufferSize);
                var lineNumber = 0;
                string? line;

                while ((line = reader.ReadLine()) != null && rowsRead < take)
                {
                    currentRowId = startRowInFile + lineNumber;
                    
                    if (lineNumber < skipInFile)
                    {
                        lineNumber++;
                        continue;
                    }

                    if (rowsRead >= take)
                        break;

                    Dictionary<string, object?>? row = null;
                    try
                    {
                        row = JsonConvert.DeserializeObject<Dictionary<string, object?>>(line);
                    }
                    catch (JsonException ex)
                    {
                        throw new InvalidOperationException($"Failed to deserialize row {currentRowId} in table '{tableName}': {ex.Message}");
                    }

                    if (row != null)
                    {
                        yield return (currentRowId, row);
                        rowsRead++;
                    }

                    lineNumber++;
                }
            }
        }

        /// <summary>
        /// Updates a specific row by row ID (expensive operation - requires rewriting file)
        /// </summary>
        public void UpdateRow(string tableName, int rowId, Dictionary<string, object?> newRow, ScalableTableMetadata metadata)
        {
            var fileIndex = rowId / _rowsPerFile;
            var rowInFile = rowId % _rowsPerFile;
            var filePath = GetTableDataFilePath(tableName, fileIndex);
            
            if (!File.Exists(filePath))
                throw new InvalidOperationException($"Row {rowId} not found in table '{tableName}'");

            var tempFilePath = filePath + ".tmp";
            
            try
            {
                using (var reader = new StreamReader(filePath))
                using (var writer = new StreamWriter(tempFilePath))
                {
                    var lineNumber = 0;
                    string? line;

                    while ((line = reader.ReadLine()) != null)
                    {
                        if (lineNumber == rowInFile)
                        {
                            var json = JsonConvert.SerializeObject(newRow, Formatting.None);
                            writer.WriteLine(json);
                        }
                        else
                        {
                            writer.WriteLine(line);
                        }
                        lineNumber++;
                    }
                }

                File.Replace(tempFilePath, filePath, null);
                metadata.LastModified = DateTime.UtcNow;
                SaveTableMetadata(tableName, metadata);
            }
            catch (Exception ex)
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
                throw new InvalidOperationException($"Failed to update row {rowId} in table '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Deletes a table and all its data files
        /// </summary>
        public void DeleteTable(string tableName)
        {
            var metaFilePath = GetTableMetaFilePath(tableName);
            if (File.Exists(metaFilePath))
                File.Delete(metaFilePath);

            // Delete all data files
            var dataPattern = $"{tableName}_data_*.jsonl";
            var dataFiles = Directory.GetFiles(_dataDirectory, dataPattern);
            foreach (var dataFile in dataFiles)
            {
                File.Delete(dataFile);
            }

            // Delete all indexes
            var indexPattern = $"{tableName}_*.json";
            var indexFiles = Directory.GetFiles(_indexDirectory, indexPattern);
            foreach (var indexFile in indexFiles)
            {
                File.Delete(indexFile);
            }
        }

        /// <summary>
        /// Gets all table names from metadata files
        /// </summary>
        public List<string> GetTableNames()
        {
            if (!Directory.Exists(_metaDirectory))
                return new List<string>();

            return Directory.GetFiles(_metaDirectory, "*_meta.json")
                .Select(file => Path.GetFileNameWithoutExtension(file))
                .Select(name => name.Substring(0, name.Length - 5)) // Remove "_meta" suffix
                .ToList();
        }

        /// <summary>
        /// Loads index data from file
        /// </summary>
        public Dictionary<object, List<int>>? LoadIndex(string tableName, string columnName)
        {
            var filePath = GetIndexFilePath(tableName, columnName);
            if (!File.Exists(filePath))
                return null;

            try
            {
                var json = File.ReadAllText(filePath);
                return JsonConvert.DeserializeObject<Dictionary<object, List<int>>>(json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to load index '{tableName}.{columnName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Saves index data to file
        /// </summary>
        public void SaveIndex(string tableName, string columnName, Dictionary<object, List<int>> indexData)
        {
            var filePath = GetIndexFilePath(tableName, columnName);
            try
            {
                var json = JsonConvert.SerializeObject(indexData, Formatting.Indented);
                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to save index '{tableName}.{columnName}': {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Metadata for scalable table storage
    /// </summary>
    public class ScalableTableMetadata
    {
        public string TableName { get; set; } = string.Empty;
        public List<ColumnFileData> Columns { get; set; } = new List<ColumnFileData>();
        public int TotalRows { get; set; } = 0;
        public int NextRowId { get; set; } = 1; // Auto-incrementing row ID
        public bool HasPrimaryKey { get; set; } = false;
        public string? PrimaryKeyColumn { get; set; }
        public Dictionary<string, long> AutoIncrementValues { get; set; } = new Dictionary<string, long>();
        public DateTime Created { get; set; } = DateTime.UtcNow;
        public DateTime LastModified { get; set; } = DateTime.UtcNow;
        public long EstimatedSizeBytes { get; set; } = 0;
    }
}
