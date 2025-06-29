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
        private readonly string _dataDirectory;
        private readonly string _indexDirectory;
        private readonly string _metaDirectory;
        private readonly int _rowsPerFile;
        private readonly int _bufferSize;

        // Binary format markers
        private static readonly byte[] BINARY_HEADER = Encoding.UTF8.GetBytes("SQLBIN");
        private const byte STRING_MARKER = 0x01;
        private const byte INTEGER_MARKER = 0x02;
        private const byte LONG_MARKER = 0x03;
        private const byte REAL_MARKER = 0x04;
        private const byte NULL_MARKER = 0x00;
        private const byte ROW_SEPARATOR = 0xFF;

        public HighPerformanceStorageManager(string baseDirectory = "fast_data", int rowsPerFile = 50000, int bufferSize = 4 * 1024 * 1024)
        {
            _dataDirectory = Path.Combine(baseDirectory, "tables");
            _indexDirectory = Path.Combine(baseDirectory, "indexes");
            _metaDirectory = Path.Combine(baseDirectory, "metadata");
            _rowsPerFile = rowsPerFile;
            _bufferSize = bufferSize;
            
            Directory.CreateDirectory(_dataDirectory);
            Directory.CreateDirectory(_indexDirectory);
            Directory.CreateDirectory(_metaDirectory);
        }

        /// <summary>
        /// Gets the binary data file path for a table and file index
        /// </summary>
        public string GetTableDataFilePath(string tableName, int fileIndex)
        {
            return Path.Combine(_dataDirectory, $"{tableName}_data_{fileIndex:D6}.bin");
        }

        /// <summary>
        /// Gets the CSV data file path for a table and file index (alternative format)
        /// </summary>
        public string GetTableCsvFilePath(string tableName, int fileIndex)
        {
            return Path.Combine(_dataDirectory, $"{tableName}_data_{fileIndex:D6}.csv");
        }

        /// <summary>
        /// Gets the data directory path for a table
        /// </summary>
        public string GetTableDirectory(string tableName)
        {
            return _dataDirectory;
        }

        /// <summary>
        /// Appends a row using ultra-fast binary format
        /// </summary>
        public int AppendRowBinary(string tableName, Dictionary<string, object?> row, ScalableTableMetadata metadata)
        {
            var fileIndex = metadata.TotalRows / _rowsPerFile;
            var filePath = GetTableDataFilePath(tableName, fileIndex);
            
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
        /// Appends a row using fast CSV format (human-readable alternative)
        /// </summary>
        public int AppendRowCsv(string tableName, Dictionary<string, object?> row, ScalableTableMetadata metadata)
        {
            var fileIndex = metadata.TotalRows / _rowsPerFile;
            var filePath = GetTableCsvFilePath(tableName, fileIndex);
            
            try
            {
                using var stream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read, _bufferSize);
                using var writer = new StreamWriter(stream, Encoding.UTF8, _bufferSize);
                
                // Write header if file is new
                if (metadata.TotalRows % _rowsPerFile == 0 && new FileInfo(filePath).Length == 0)
                {
                    var headers = string.Join(",", metadata.Columns.Select(c => c.Name));
                    writer.WriteLine(headers);
                }
                
                // Write CSV row
                var values = new List<string>();
                foreach (var column in metadata.Columns)
                {
                    if (row.TryGetValue(column.Name, out var value))
                    {
                        values.Add(EscapeCsvValue(value?.ToString() ?? ""));
                    }
                    else
                    {
                        values.Add("");
                    }
                }
                
                writer.WriteLine(string.Join(",", values));
                
                var rowId = metadata.TotalRows;
                metadata.TotalRows++;
                metadata.LastModified = DateTime.UtcNow;
                
                return rowId;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to append CSV row to table '{tableName}': {ex.Message}");
            }
        }

        /// <summary>
        /// Reads rows using ultra-fast binary format
        /// </summary>
        public IEnumerable<(int rowId, Dictionary<string, object?> row)> ReadRowsBinary(string tableName, ScalableTableMetadata metadata, int skip = 0, int take = int.MaxValue)
        {
            var rowsRead = 0;
            var startFileIndex = skip / _rowsPerFile;
            var endFileIndex = Math.Min((skip + take - 1) / _rowsPerFile, (metadata.TotalRows - 1) / _rowsPerFile);

            for (var fileIndex = startFileIndex; fileIndex <= endFileIndex && rowsRead < take; fileIndex++)
            {
                var filePath = GetTableDataFilePath(tableName, fileIndex);
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
        /// Reads rows using fast CSV format
        /// </summary>
        public IEnumerable<(int rowId, Dictionary<string, object?>)> ReadRowsCsv(
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
                var filePath = GetTableCsvFilePath(tableName, fileIndex);
                if (!File.Exists(filePath))
                    continue;

                var startRowInFile = fileIndex * _rowsPerFile;
                var skipInFile = Math.Max(0, skip - startRowInFile);

                using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, _bufferSize);
                using var reader = new StreamReader(stream, Encoding.UTF8, bufferSize: _bufferSize);

                // Skip header
                reader.ReadLine();
                
                var rowsInFile = 0;
                string? line;

                while ((line = reader.ReadLine()) != null && rowsRead < take)
                {
                    currentRowId = startRowInFile + rowsInFile;
                    
                    if (rowsInFile < skipInFile)
                    {
                        rowsInFile++;
                        continue;
                    }

                    if (rowsRead >= take)
                        break;

                    Dictionary<string, object?>? row = null;
                    try
                    {
                        row = ParseCsvRow(line, metadata.Columns);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Failed to parse CSV row {currentRowId} in table '{tableName}': {ex.Message}");
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
        /// Parses a CSV row into a dictionary
        /// </summary>
        private Dictionary<string, object?> ParseCsvRow(string csvLine, List<ColumnFileData> columns)
        {
            var values = ParseCsvLine(csvLine);
            var row = new Dictionary<string, object?>();

            for (int i = 0; i < columns.Count && i < values.Count; i++)
            {
                var column = columns[i];
                var value = values[i];

                if (string.IsNullOrEmpty(value))
                {
                    row[column.Name] = null;
                    continue;
                }

                switch (column.DataType.ToUpper())
                {
                    case "INTEGER":
                        row[column.Name] = int.TryParse(value, out var intVal) ? intVal : 0;
                        break;
                    case "LONG":
                        row[column.Name] = long.TryParse(value, out var longVal) ? longVal : 0L;
                        break;
                    case "REAL":
                        row[column.Name] = double.TryParse(value, out var doubleVal) ? doubleVal : 0.0;
                        break;
                    default:
                        row[column.Name] = value;
                        break;
                }
            }

            return row;
        }

        /// <summary>
        /// Fast CSV line parser
        /// </summary>
        private List<string> ParseCsvLine(string line)
        {
            var values = new List<string>();
            var current = new StringBuilder();
            var inQuotes = false;

            for (int i = 0; i < line.Length; i++)
            {
                var ch = line[i];

                if (ch == '"')
                {
                    inQuotes = !inQuotes;
                }
                else if (ch == ',' && !inQuotes)
                {
                    values.Add(current.ToString());
                    current.Clear();
                }
                else
                {
                    current.Append(ch);
                }
            }

            values.Add(current.ToString());
            return values;
        }

        /// <summary>
        /// Escapes CSV values
        /// </summary>
        private string EscapeCsvValue(string value)
        {
            if (string.IsNullOrEmpty(value))
                return "";

            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n"))
            {
                return $"\"{value.Replace("\"", "\"\"")}\"";
            }

            return value;
        }

        /// <summary>
        /// Gets performance statistics for a table
        /// </summary>
        public Dictionary<string, object> GetPerformanceStats(string tableName, ScalableTableMetadata metadata)
        {
            var binaryFiles = Directory.GetFiles(_dataDirectory, $"{tableName}_data_*.bin");
            var csvFiles = Directory.GetFiles(_dataDirectory, $"{tableName}_data_*.csv");
            
            var binarySize = binaryFiles.Sum(f => new FileInfo(f).Length);
            var csvSize = csvFiles.Sum(f => new FileInfo(f).Length);

            return new Dictionary<string, object>
            {
                ["TableName"] = tableName,
                ["TotalRows"] = metadata.TotalRows,
                ["BinaryFiles"] = binaryFiles.Length,
                ["CsvFiles"] = csvFiles.Length,
                ["BinarySizeBytes"] = binarySize,
                ["CsvSizeBytes"] = csvSize,
                ["BinarySizeMB"] = binarySize / (1024.0 * 1024.0),
                ["CsvSizeMB"] = csvSize / (1024.0 * 1024.0),
                ["CompressionRatio"] = csvSize > 0 ? (double)binarySize / csvSize : 0,
                ["RowsPerFile"] = _rowsPerFile,
                ["BufferSizeKB"] = _bufferSize / 1024
            };
        }

        /// <summary>
        /// Loads table metadata (same as before)
        /// </summary>
        public ScalableTableMetadata? LoadTableMetadata(string tableName)
        {
            var filePath = Path.Combine(_metaDirectory, $"{tableName}_meta.json");
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
        public void SaveTableMetadata(string tableName, ScalableTableMetadata metadata)
        {
            var filePath = Path.Combine(_metaDirectory, $"{tableName}_meta.json");
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
        public List<string> GetTableNames()
        {
            var tableNames = new List<string>();
            
            if (!Directory.Exists(_metaDirectory))
                return tableNames;

            var metadataFiles = Directory.GetFiles(_metaDirectory, "*.json");
            foreach (var file in metadataFiles)
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                if (fileName.EndsWith("_metadata"))
                {
                    var tableName = fileName.Substring(0, fileName.Length - "_metadata".Length);
                    tableNames.Add(tableName);
                }
            }
            
            return tableNames;
        }

        /// <summary>
        /// Deletes a table and all its files
        /// </summary>
        public void DeleteTable(string tableName)
        {
            // Delete metadata file
            var metadataFile = GetTableMetadataFilePath(tableName);
            if (File.Exists(metadataFile))
                File.Delete(metadataFile);

            // Delete all data files
            var dataFiles = Directory.GetFiles(_dataDirectory, $"{tableName}_data_*.bin");
            foreach (var file in dataFiles)
                File.Delete(file);
                
            var csvFiles = Directory.GetFiles(_dataDirectory, $"{tableName}_data_*.csv");
            foreach (var file in csvFiles)
                File.Delete(file);
        }

        /// <summary>
        /// Gets the metadata file path for a table
        /// </summary>
        public string GetTableMetadataFilePath(string tableName)
        {
            return Path.Combine(_metaDirectory, $"{tableName}_metadata.json");
        }

        /// <summary>
        /// Performs efficient batch update/delete operations using a streaming approach
        /// Returns the number of rows that were modified or deleted
        /// </summary>
        public int ProcessRowsBatch(string tableName, ScalableTableMetadata metadata, 
            Func<Dictionary<string, object?>, bool> shouldUpdate, 
            Func<Dictionary<string, object?>, Dictionary<string, object?>>? updateFunction = null)
        {
            var tempDir = Path.Combine(Path.GetTempPath(), $"sql_temp_{Guid.NewGuid():N}");
            Directory.CreateDirectory(tempDir);
            
            try
            {
                var totalFiles = (metadata.TotalRows + _rowsPerFile - 1) / _rowsPerFile;
                var newTotalRows = 0;
                var hasChanges = false;
                var modifiedRowCount = 0;

                // Process each file
                for (int fileIndex = 0; fileIndex < totalFiles; fileIndex++)
                {
                    var sourceFilePath = GetTableCsvFilePath(tableName, fileIndex);
                    if (!File.Exists(sourceFilePath)) continue;

                    var tempFilePath = Path.Combine(tempDir, $"temp_{fileIndex:D6}.csv");
                    var fileHasChanges = false;
                    var rowsInThisFile = 0;

                    using (var reader = new StreamReader(sourceFilePath, Encoding.UTF8))
                    using (var writer = new StreamWriter(tempFilePath, false, Encoding.UTF8, _bufferSize))
                    {
                        // Copy header
                        var header = reader.ReadLine();
                        if (header != null)
                        {
                            writer.WriteLine(header);
                        }

                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            var row = ParseCsvRow(line, metadata.Columns);
                            
                            if (shouldUpdate(row))
                            {
                                modifiedRowCount++;
                                if (updateFunction != null)
                                {
                                    // Update operation
                                    var updatedRow = updateFunction(row);
                                    var csvLine = CreateCsvLine(updatedRow, metadata.Columns);
                                    writer.WriteLine(csvLine);
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
                                writer.WriteLine(line);
                                rowsInThisFile++;
                            }
                        }
                    }

                    if (fileHasChanges)
                    {
                        hasChanges = true;
                    }

                    newTotalRows += rowsInThisFile;

                    // Only move temp file if we have rows or this is the first file (to preserve header)
                    if (rowsInThisFile > 0 || fileIndex == 0)
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
                    SaveTableMetadata(tableName, metadata);
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
        /// Efficiently processes rows in binary format using batch operations
        /// Minimizes memory usage and only rewrites files that have changes
        /// Returns the number of rows that were modified or deleted
        /// </summary>
        public int ProcessRowsBatchBinary(string tableName, ScalableTableMetadata metadata, 
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
                    var sourceFilePath = GetTableDataFilePath(tableName, fileIndex);
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
                    SaveTableMetadata(tableName, metadata);
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
        /// Creates a CSV line from a row dictionary
        /// </summary>
        private string CreateCsvLine(Dictionary<string, object?> row, List<ColumnFileData> columns)
        {
            var values = new List<string>();
            foreach (var column in columns)
            {
                if (row.TryGetValue(column.Name, out var value))
                {
                    values.Add(EscapeCsvValue(value?.ToString() ?? ""));
                }
                else
                {
                    values.Add("");
                }
            }
            return string.Join(",", values);
        }

        /// <summary>
        /// Parses CSV values from a line, handling quoted values
        /// </summary>
        private List<string> ParseCsvValues(string csvLine)
        {
            var values = new List<string>();
            var current = new StringBuilder();
            var inQuotes = false;
            
            for (int i = 0; i < csvLine.Length; i++)
            {
                var ch = csvLine[i];
                
                if (ch == '"')
                {
                    if (inQuotes && i + 1 < csvLine.Length && csvLine[i + 1] == '"')
                    {
                        // Escaped quote
                        current.Append('"');
                        i++; // Skip next quote
                    }
                    else
                    {
                        inQuotes = !inQuotes;
                    }
                }
                else if (ch == ',' && !inQuotes)
                {
                    values.Add(current.ToString());
                    current.Clear();
                }
                else
                {
                    current.Append(ch);
                }
            }
            
            values.Add(current.ToString());
            return values;
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
                "TEXT" or "STRING" or "VARCHAR" => value,
                _ => value
            };
        }
    }
}
