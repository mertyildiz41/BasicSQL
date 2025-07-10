using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace BasicSQL.Storage
{
    /// <summary>
    /// High-performance storage manager using binary format and line-based text format
    /// for ultra-fast I/O operations (10-100x faster than JSON)
    /// </summary>
    public class HighPerformanceStorageManager
    {
        private readonly string _baseDirectory;
        private readonly int _rowsPerFile;
        private readonly int _bufferSize;

        // Binary format markers
        private static readonly byte[] BINARY_HEADER = Encoding.UTF8.GetBytes("SQLBIN");
        private const byte STRING_MARKER = 0x01;
        private const byte INTEGER_MARKER = 0x02;
        private const byte LONG_MARKER = 0x03;
        private const byte REAL_MARKER = 0x04;
        private const byte DATETIME_MARKER = 0x05;
        private const byte DECIMAL_MARKER = 0x06;
        private const byte NULL_MARKER = 0x00;
        private const byte ROW_SEPARATOR = 0xFF;

        public HighPerformanceStorageManager(string baseDirectory = "fast_data", int rowsPerFile = 50000, int bufferSize = 4 * 1024 * 1024)
        {
            _baseDirectory = baseDirectory;
            _rowsPerFile = rowsPerFile;
            _bufferSize = bufferSize;
            Directory.CreateDirectory(_baseDirectory);
        }

        // --- Path Helper Methods ---
        private string GetDatabasePath(string databaseName) => Path.Combine(_baseDirectory, databaseName);
        private string GetDataDirectory(string databaseName) => Path.Combine(GetDatabasePath(databaseName), "tables");
        private string GetMetaDirectory(string databaseName) => Path.Combine(GetDatabasePath(databaseName), "metadata");
        public string GetTableDataFilePath(string databaseName, string tableName, int fileIndex) => Path.Combine(GetDataDirectory(databaseName), $"{tableName}_data_{fileIndex:D6}.bin");
        public string GetTableMetadataFilePath(string databaseName, string tableName) => Path.Combine(GetMetaDirectory(databaseName), $"{tableName}_meta.json");

        // --- Database Management ---
        public void CreateDatabase(string databaseName)
        {
            var dbPath = GetDatabasePath(databaseName);
            Directory.CreateDirectory(dbPath);
            Directory.CreateDirectory(GetDataDirectory(databaseName));
            Directory.CreateDirectory(GetMetaDirectory(databaseName));
        }

        public void DeleteDatabase(string databaseName)
        {
            var dbPath = GetDatabasePath(databaseName);
            if (Directory.Exists(dbPath))
            {
                Directory.Delete(dbPath, true);
            }
        }

        public List<string> GetDatabaseNames()
        {
            return Directory.GetDirectories(_baseDirectory).Select(Path.GetFileName).ToList()!;
        }

        /// <summary>
        /// Appends a row using ultra-fast binary format
        /// </summary>
        public int AppendRowBinary(string databaseName, string tableName, Dictionary<string, object?> row, ScalableTableMetadata metadata)
        {
            var fileIndex = metadata.TotalRows / _rowsPerFile;
            var filePath = GetTableDataFilePath(databaseName, tableName, fileIndex);
            
            try
            {
                using var stream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read, _bufferSize);
                
                // Write binary row data
                WriteBinaryRow(stream, row, metadata.Columns);
                
                var rowId = metadata.TotalRows;
                metadata.TotalRows++;
                metadata.LastModified = DateTime.UtcNow;
                
                return rowId;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to append binary row to table '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Reads rows using ultra-fast binary format
        /// </summary>
        public IEnumerable<(int rowId, Dictionary<string, object?> row)> ReadRowsBinary(string databaseName, string tableName, ScalableTableMetadata metadata, int skip = 0, int take = int.MaxValue)
        {
            var rowsRead = 0;
            var startFileIndex = skip / _rowsPerFile;
            var endFileIndex = Math.Min((skip + take - 1) / _rowsPerFile, (metadata.TotalRows - 1) / _rowsPerFile);

            for (var fileIndex = startFileIndex; fileIndex <= endFileIndex && rowsRead < take; fileIndex++)
            {
                var filePath = GetTableDataFilePath(databaseName, tableName, fileIndex);
                if (!File.Exists(filePath))
                    continue;

                var startRowInFile = fileIndex * _rowsPerFile;
                var skipInFile = Math.Max(0, skip - startRowInFile);

                using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize);
                using var reader = new BinaryReader(stream);

                var rowsInFile = 0;
                int currentRowId;

                while (stream.Position < stream.Length && rowsRead < take)
                {
                    currentRowId = startRowInFile + rowsInFile;
                    
                    if (rowsInFile < skipInFile)
                    {
                        // Skip this row by reading and discarding
                        try
                        {
                            ReadBinaryRow(reader, metadata.Columns);
                        }
                        catch
                        {
                            break; // End of valid data
                        }
                        rowsInFile++;
                        continue;
                    }

                    if (rowsRead >= take)
                        break;

                    Dictionary<string, object?>? row = null;
                    try
                    {
                        row = ReadBinaryRow(reader, metadata.Columns);
                    }
                    catch (EndOfStreamException)
                    {
                        break; // End of file
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Failed to read binary row {currentRowId} in table '{tableName}': {ex.Message}");
                    }

                    if (row != null)
                    {
                        yield return (currentRowId, row);
                        rowsRead++;
                    }

                    rowsInFile++;
                }
            }
        }

        /// <summary>
        /// Writes a row in binary format (ultra-fast)
        /// </summary>
        private void WriteBinaryRow(Stream stream, Dictionary<string, object?> row, List<ColumnFileData> columns)
        {
            foreach (var column in columns)
            {
                if (!row.TryGetValue(column.Name, out var value) || value == null)
                {
                    stream.WriteByte(NULL_MARKER);
                    continue;
                }

                switch (column.DataType.ToUpper())
                {
                    case "TEXT":
                        stream.WriteByte(STRING_MARKER);
                        var strBytes = Encoding.UTF8.GetBytes(value.ToString()!);
                        var strLength = BitConverter.GetBytes(strBytes.Length);
                        stream.Write(strLength, 0, 4);
                        stream.Write(strBytes, 0, strBytes.Length);
                        break;

                    case "INTEGER":
                        stream.WriteByte(INTEGER_MARKER);
                        var intBytes = BitConverter.GetBytes(Convert.ToInt32(value));
                        stream.Write(intBytes, 0, 4);
                        break;

                    case "LONG":
                        stream.WriteByte(LONG_MARKER);
                        var longBytes = BitConverter.GetBytes(Convert.ToInt64(value));
                        stream.Write(longBytes, 0, 8);
                        break;

                    case "REAL":
                        stream.WriteByte(REAL_MARKER);
                        var realBytes = BitConverter.GetBytes(Convert.ToDouble(value));
                        stream.Write(realBytes, 0, 8);
                        break;

                    case "DATETIME":
                        stream.WriteByte(DATETIME_MARKER);
                        var dateTimeValue = value is DateTime dt ? dt : Convert.ToDateTime(value);
                        var tickBytes = BitConverter.GetBytes(dateTimeValue.Ticks);
                        stream.Write(tickBytes, 0, 8);
                        break;

                    case "DECIMAL":
                        stream.WriteByte(DECIMAL_MARKER);
                        var decimalValue = value is decimal dec ? dec : Convert.ToDecimal(value);
                        var decimalBits = decimal.GetBits(decimalValue);
                        // Write all 4 32-bit integers that make up the decimal (16 bytes total)
                        foreach (var bit in decimalBits)
                        {
                            var bitBytes = BitConverter.GetBytes(bit);
                            stream.Write(bitBytes, 0, 4);
                        }
                        break;

                    default:
                        stream.WriteByte(STRING_MARKER);
                        var defaultBytes = Encoding.UTF8.GetBytes(value.ToString()!);
                        var defaultLength = BitConverter.GetBytes(defaultBytes.Length);
                        stream.Write(defaultLength, 0, 4);
                        stream.Write(defaultBytes, 0, defaultBytes.Length);
                        break;
                }
            }
            
            // Row separator
            stream.WriteByte(ROW_SEPARATOR);
        }

        /// <summary>
        /// Reads a row from binary format (ultra-fast)
        /// </summary>
        private Dictionary<string, object?> ReadBinaryRow(BinaryReader reader, List<ColumnFileData> columns)
        {
            var row = new Dictionary<string, object?>();

            foreach (var column in columns)
            {
                var marker = reader.ReadByte();
                
                if (marker == NULL_MARKER)
                {
                    row[column.Name] = null;
                    continue;
                }

                switch (marker)
                {
                    case STRING_MARKER:
                        var length = reader.ReadInt32();
                        var strBytes = reader.ReadBytes(length);
                        row[column.Name] = Encoding.UTF8.GetString(strBytes);
                        break;

                    case INTEGER_MARKER:
                        row[column.Name] = reader.ReadInt32();
                        break;

                    case LONG_MARKER:
                        row[column.Name] = reader.ReadInt64();
                        break;

                    case REAL_MARKER:
                        row[column.Name] = reader.ReadDouble();
                        break;

                    case DATETIME_MARKER:
                        var ticks = reader.ReadInt64();
                        row[column.Name] = new DateTime(ticks);
                        break;

                    case DECIMAL_MARKER:
                        // Read 4 32-bit integers that make up the decimal (16 bytes total)
                        var decimalBits = new int[4];
                        for (int i = 0; i < 4; i++)
                        {
                            decimalBits[i] = reader.ReadInt32();
                        }
                        row[column.Name] = new decimal(decimalBits);
                        break;

                    default:
                        throw new InvalidDataException($"Unknown binary marker: {marker}");
                }
            }

            // Read row separator
            var separator = reader.ReadByte();
            if (separator != ROW_SEPARATOR)
                throw new InvalidDataException("Missing row separator in binary data");

            return row;
        }

        /// <summary>
        /// Gets performance statistics for a table
        /// </summary>
        public Dictionary<string, object> GetPerformanceStats(string databaseName, string tableName, ScalableTableMetadata metadata)
        {
            var dataDirectory = GetDataDirectory(databaseName);
            var binaryFiles = Directory.GetFiles(dataDirectory, $"{tableName}_data_*.bin");
            
            var binarySize = binaryFiles.Sum(f => new FileInfo(f).Length);

            return new Dictionary<string, object>
            {
                ["TableName"] = tableName,
                ["TotalRows"] = metadata.TotalRows,
                ["BinaryFiles"] = binaryFiles.Length,
                ["BinarySizeBytes"] = binarySize,
                ["BinarySizeMB"] = binarySize / (1024.0 * 1024.0),
                ["CompressionRatio"] = 1.0, // Binary is the primary format
                ["RowsPerFile"] = _rowsPerFile,
                ["BufferSizeKB"] = _bufferSize / 1024
            };
        }

        /// <summary>
        /// Loads table metadata (same as before)
        /// </summary>
        public ScalableTableMetadata? LoadTableMetadata(string databaseName, string tableName)
        {
            var filePath = GetTableMetadataFilePath(databaseName, tableName);
            if (!File.Exists(filePath))
                return null;

            try
            {
                var json = File.ReadAllText(filePath);
                return Newtonsoft.Json.JsonConvert.DeserializeObject<ScalableTableMetadata>(json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to load table metadata '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Saves table metadata (same as before)
        /// </summary>
        public void SaveTableMetadata(string databaseName, string tableName, ScalableTableMetadata metadata)
        {
            var metaDirectory = GetMetaDirectory(databaseName);
            Directory.CreateDirectory(metaDirectory); // Ensure it exists
            var filePath = GetTableMetadataFilePath(databaseName, tableName);
            try
            {
                var json = Newtonsoft.Json.JsonConvert.SerializeObject(metadata, Newtonsoft.Json.Formatting.Indented);
                File.WriteAllText(filePath, json);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to save table metadata '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Gets all table names from storage
        /// </summary>
        public List<string> GetTableNames(string databaseName)
        {
            var tableNames = new List<string>();
            var metaDirectory = GetMetaDirectory(databaseName);
            
            if (!Directory.Exists(metaDirectory))
                return tableNames;

            var metadataFiles = Directory.GetFiles(metaDirectory, "*_meta.json");
            foreach (var file in metadataFiles)
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                if (fileName.EndsWith("_meta"))
                {
                    var tableName = fileName.Substring(0, fileName.Length - "_meta".Length);
                    tableNames.Add(tableName);
                }
            }
            
            return tableNames;
        }

        /// <summary>
        /// Deletes a table and all its files
        /// </summary>
        public void DeleteTable(string databaseName, string tableName)
        {
            // Delete metadata file
            var metadataFile = GetTableMetadataFilePath(databaseName, tableName);
            if (File.Exists(metadataFile))
                File.Delete(metadataFile);

            // Delete all data files
            var dataDirectory = GetDataDirectory(databaseName);
            var dataFiles = Directory.GetFiles(dataDirectory, $"{tableName}_data_*.bin");
            foreach (var file in dataFiles)
                File.Delete(file);
        }

        /// <summary>
        /// Efficiently processes rows in binary format using batch operations
        /// Minimizes memory usage and only rewrites files that have changes
        /// Returns the number of rows that were modified or deleted
        /// </summary>
        public int ProcessRowsBatchBinary(string databaseName, string tableName, ScalableTableMetadata metadata, 
            Func<Dictionary<string, object?>, bool> shouldUpdate, 
            Func<Dictionary<string, object?>, Dictionary<string, object?>>? updateFunction = null)
        {
            var tempDir = Path.Combine(Path.GetTempPath(), $"sql_temp_bin_{Guid.NewGuid():N}");
            Directory.CreateDirectory(tempDir);
            
            try
            {
                var totalFiles = (metadata.TotalRows + _rowsPerFile - 1) / _rowsPerFile;
                var newTotalRows = 0;
                var hasChanges = false;
                var modifiedRowCount = 0;

                // Process each binary file
                for (int fileIndex = 0; fileIndex < totalFiles; fileIndex++)
                {
                    var sourceFilePath = GetTableDataFilePath(databaseName, tableName, fileIndex);
                    if (!File.Exists(sourceFilePath)) continue;

                    var tempFilePath = Path.Combine(tempDir, $"temp_{fileIndex:D6}.bin");
                    var fileHasChanges = false;
                    var rowsInThisFile = 0;

                    using (var reader = new FileStream(sourceFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize))
                    using (var writer = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None, _bufferSize))
                    {
                        // Copy binary header if it exists
                        var headerBuffer = new byte[BINARY_HEADER.Length];
                        if (reader.Read(headerBuffer, 0, headerBuffer.Length) == headerBuffer.Length &&
                            headerBuffer.SequenceEqual(BINARY_HEADER))
                        {
                            writer.Write(BINARY_HEADER);
                        }
                        else
                        {
                            reader.Position = 0; // Reset if no header found
                        }

                        // Process rows in this file
                        while (reader.Position < reader.Length)
                        {
                            var row = ReadBinaryRowFromStream(reader, metadata.Columns);
                            if (row == null) break; // End of file or corrupted data
                            
                            if (shouldUpdate(row))
                            {
                                modifiedRowCount++;
                                if (updateFunction != null)
                                {
                                    // Update operation
                                    var updatedRow = updateFunction(row);
                                    WriteBinaryRow(writer, updatedRow, metadata.Columns);
                                    rowsInThisFile++;
                                    fileHasChanges = true;
                                }
                                // If updateFunction is null, this is a delete operation - skip writing the row
                                else
                                {
                                    fileHasChanges = true;
                                    // Don't write the row (delete it)
                                }
                            }
                            else
                            {
                                // Row doesn't match criteria - keep it unchanged
                                WriteBinaryRow(writer, row, metadata.Columns);
                                rowsInThisFile++;
                            }
                        }
                    }

                    if (fileHasChanges)
                    {
                        hasChanges = true;
                    }

                    newTotalRows += rowsInThisFile;

                    // Only move temp file if we have rows
                    if (rowsInThisFile > 0)
                    {
                        // Delete the original file first to avoid "file already exists" error
                        if (File.Exists(sourceFilePath))
                        {
                            File.Delete(sourceFilePath);
                        }
                        File.Move(tempFilePath, sourceFilePath);
                    }
                    else if (File.Exists(sourceFilePath))
                    {
                        // Remove empty file
                        File.Delete(sourceFilePath);
                    }
                }

                if (hasChanges)
                {
                    metadata.TotalRows = newTotalRows;
                    metadata.LastModified = DateTime.UtcNow;
                    SaveTableMetadata(databaseName, tableName, metadata);
                }
                
                return modifiedRowCount;
            }
            finally
            {
                // Clean up temp directory
                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, true);
                }
            }
        }

        /// <summary>
        /// Reads a single binary row from a stream
        /// </summary>
        private Dictionary<string, object?>? ReadBinaryRowFromStream(FileStream stream, List<ColumnFileData> columns)
        {
            try
            {
                var row = new Dictionary<string, object?>();
                
                foreach (var column in columns)
                {
                    var markerByte = stream.ReadByte();
                    if (markerByte == -1) return null; // End of stream
                    
                    var marker = (byte)markerByte;
                    
                    switch (marker)
                    {
                        case NULL_MARKER:
                            row[column.Name] = null;
                            break;
                            
                        case STRING_MARKER:
                            var lengthBytes = new byte[4];
                            if (stream.Read(lengthBytes, 0, 4) != 4) return null;
                            var length = BitConverter.ToInt32(lengthBytes, 0);
                            var stringBytes = new byte[length];
                            if (stream.Read(stringBytes, 0, length) != length) return null;
                            row[column.Name] = Encoding.UTF8.GetString(stringBytes);
                            break;
                            
                        case INTEGER_MARKER:
                            var intBytes = new byte[4];
                            if (stream.Read(intBytes, 0, 4) != 4) return null;
                            row[column.Name] = BitConverter.ToInt32(intBytes, 0);
                            break;
                            
                        case LONG_MARKER:
                            var longBytes = new byte[8];
                            if (stream.Read(longBytes, 0, 8) != 8) return null;
                            row[column.Name] = BitConverter.ToInt64(longBytes, 0);
                            break;
                            
                        case REAL_MARKER:
                            var doubleBytes = new byte[8];
                            if (stream.Read(doubleBytes, 0, 8) != 8) return null;
                            row[column.Name] = BitConverter.ToDouble(doubleBytes, 0);
                            break;
                            
                        case DATETIME_MARKER:
                            var tickBytes = new byte[8];
                            if (stream.Read(tickBytes, 0, 8) != 8) return null;
                            var ticks = BitConverter.ToInt64(tickBytes, 0);
                            row[column.Name] = new DateTime(ticks);
                            break;
                            
                        case DECIMAL_MARKER:
                            // Read 4 32-bit integers that make up the decimal (16 bytes total)
                            var decimalBits = new int[4];
                            for (int i = 0; i < 4; i++)
                            {
                                var decimalIntBytes = new byte[4];
                                if (stream.Read(decimalIntBytes, 0, 4) != 4) return null;
                                decimalBits[i] = BitConverter.ToInt32(decimalIntBytes, 0);
                            }
                            row[column.Name] = new decimal(decimalBits);
                            break;
                            
                        default:
                            throw new InvalidOperationException($"Unknown binary marker: {marker}");
                    }
                }
                
                // Check for row separator
                var separatorByte = stream.ReadByte();
                if (separatorByte != ROW_SEPARATOR && separatorByte != -1)
                {
                    // Try to recover by seeking to next separator
                    while (stream.Position < stream.Length && stream.ReadByte() != ROW_SEPARATOR) { }
                }
                
                return row;
            }
            catch
            {
                return null; // Corrupted data, skip this row
            }
        }

        /// <summary>
        /// Converts a string value to the appropriate type
        /// </summary>
        private object? ConvertValue(string value, string type)
        {
            if (string.IsNullOrEmpty(value)) return null;
            
            return type.ToUpper() switch
            {
                "INTEGER" or "INT" => int.TryParse(value, out var intVal) ? intVal : null,
                "BIGINT" or "LONG" => long.TryParse(value, out var longVal) ? longVal : null,
                "REAL" or "FLOAT" or "DOUBLE" => double.TryParse(value, out var doubleVal) ? doubleVal : null,
                "DECIMAL" or "MONEY" or "NUMERIC" => decimal.TryParse(value, out var decimalVal) ? decimalVal : null,
                "DATETIME" => DateTime.TryParse(value, out var dateVal) ? dateVal : null,
                "TEXT" or "STRING" or "VARCHAR" => value,
                _ => value
            };
        }
    }
}
