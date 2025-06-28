using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace BasicSQL.Storage
{
    /// <summary>
    /// File-based storage manager for tables and indexes
    /// </summary>
    public class FileStorageManager
    {
        private readonly string _dataDirectory;
        private readonly string _indexDirectory;

        public FileStorageManager(string baseDirectory = "data")
        {
            _dataDirectory = Path.Combine(baseDirectory, "tables");
            _indexDirectory = Path.Combine(baseDirectory, "indexes");
            
            // Ensure directories exist
            Directory.CreateDirectory(_dataDirectory);
            Directory.CreateDirectory(_indexDirectory);
        }

        /// <summary>
        /// Gets the file path for a table
        /// </summary>
        public string GetTableFilePath(string tableName)
        {
            return Path.Combine(_dataDirectory, $"{tableName}.json");
        }

        /// <summary>
        /// Gets the file path for an index
        /// </summary>
        public string GetIndexFilePath(string tableName, string columnName)
        {
            return Path.Combine(_indexDirectory, $"{tableName}_{columnName}.json");
        }

        /// <summary>
        /// Loads table data from file
        /// </summary>
        public TableFileData? LoadTable(string tableName)
        {
            var filePath = GetTableFilePath(tableName);
            if (!File.Exists(filePath))
                return null;

            try
            {
                var json = File.ReadAllText(filePath);
                return JsonConvert.DeserializeObject<TableFileData>(json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to load table '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Saves table data to file
        /// </summary>
        public void SaveTable(string tableName, TableFileData tableData)
        {
            var filePath = GetTableFilePath(tableName);
            try
            {
                var json = JsonConvert.SerializeObject(tableData, Formatting.Indented);
                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to save table '{tableName}': {ex.Message}");
            }
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

        /// <summary>
        /// Gets all table names from files
        /// </summary>
        public List<string> GetTableNames()
        {
            if (!Directory.Exists(_dataDirectory))
                return new List<string>();

            return Directory.GetFiles(_dataDirectory, "*.json")
                .Select(file => Path.GetFileNameWithoutExtension(file))
                .ToList();
        }

        /// <summary>
        /// Deletes a table file
        /// </summary>
        public void DeleteTable(string tableName)
        {
            var filePath = GetTableFilePath(tableName);
            if (File.Exists(filePath))
                File.Delete(filePath);

            // Also delete any indexes for this table
            var indexPattern = $"{tableName}_*.json";
            var indexFiles = Directory.GetFiles(_indexDirectory, indexPattern);
            foreach (var indexFile in indexFiles)
            {
                File.Delete(indexFile);
            }
        }

        /// <summary>
        /// Deletes an index file
        /// </summary>
        public void DeleteIndex(string tableName, string columnName)
        {
            var filePath = GetIndexFilePath(tableName, columnName);
            if (File.Exists(filePath))
                File.Delete(filePath);
        }

        /// <summary>
        /// Gets all index names for a table
        /// </summary>
        public List<string> GetIndexNames(string tableName)
        {
            if (!Directory.Exists(_indexDirectory))
                return new List<string>();

            var pattern = $"{tableName}_*.json";
            return Directory.GetFiles(_indexDirectory, pattern)
                .Select(file => Path.GetFileNameWithoutExtension(file))
                .Select(name => name.Substring(tableName.Length + 1)) // Remove "tableName_" prefix
                .ToList();
        }
    }

    /// <summary>
    /// Data structure for table file storage
    /// </summary>
    public class TableFileData
    {
        public string TableName { get; set; } = string.Empty;
        public List<ColumnFileData> Columns { get; set; } = new List<ColumnFileData>();
        public List<Dictionary<string, object?>> Rows { get; set; } = new List<Dictionary<string, object?>>();
        public int NextRowId { get; set; } = 1; // Auto-incrementing row ID
        public bool HasPrimaryKey { get; set; } = false;
        public string? PrimaryKeyColumn { get; set; }
        public Dictionary<string, long> AutoIncrementValues { get; set; } = new Dictionary<string, long>(); // Next auto-increment value per column
    }

    /// <summary>
    /// Data structure for column file storage
    /// </summary>
    public class ColumnFileData
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public bool IsNullable { get; set; } = true;
        public bool IsPrimaryKey { get; set; } = false;
        public bool IsAutoIncrement { get; set; } = false;
    }
}
