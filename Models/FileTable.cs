using System;
using System.Collections.Generic;
using System.Linq;
using BasicSQL.Models;
using BasicSQL.Storage;

namespace BasicSQL.Models
{
    /// <summary>
    /// File-based table implementation with auto-incrementing row IDs and index support
    /// </summary>
    public class FileTable
    {
        private readonly FileStorageManager _storageManager;
        private TableFileData _tableData;
        private readonly Dictionary<string, Dictionary<object, List<int>>> _indexes;

        public string Name => _tableData.TableName;
        public List<Column> Columns { get; private set; } = new List<Column>();
        public int RowCount => _tableData.Rows.Count;
        public bool HasPrimaryKey => _tableData.HasPrimaryKey;

        public FileTable(string tableName, List<Column> columns, FileStorageManager storageManager)
        {
            _storageManager = storageManager;
            _indexes = new Dictionary<string, Dictionary<object, List<int>>>();
            
            // Initialize table data
            _tableData = new TableFileData
            {
                TableName = tableName,
                Columns = columns.Select(c => new ColumnFileData
                {
                    Name = c.Name,
                    DataType = c.DataType.ToString(),
                    IsNullable = c.IsNullable,
                    IsPrimaryKey = c.IsPrimaryKey,
                    IsAutoIncrement = c.IsAutoIncrement
                }).ToList(),
                HasPrimaryKey = columns.Any(c => c.IsPrimaryKey),
                PrimaryKeyColumn = columns.FirstOrDefault(c => c.IsPrimaryKey)?.Name,
                AutoIncrementValues = columns.Where(c => c.IsAutoIncrement)
                    .ToDictionary(c => c.Name, c => 1L) // Initialize auto-increment values to 1
            };

            Columns = columns;
            SaveToFile();
        }

        /// <summary>
        /// Loads an existing table from file
        /// </summary>
        public static FileTable? LoadFromFile(string tableName, FileStorageManager storageManager)
        {
            var tableData = storageManager.LoadTable(tableName);
            if (tableData == null)
                return null;

            var table = new FileTable(storageManager, tableData);
            return table;
        }

        /// <summary>
        /// Private constructor for loading from file
        /// </summary>
        private FileTable(FileStorageManager storageManager, TableFileData tableData)
        {
            _storageManager = storageManager;
            _tableData = tableData;
            _indexes = new Dictionary<string, Dictionary<object, List<int>>>();

            // Ensure AutoIncrementValues is initialized if missing (for backward compatibility)
            if (_tableData.AutoIncrementValues == null)
                _tableData.AutoIncrementValues = new Dictionary<string, long>();

            // Convert file data back to Column objects
            Columns = tableData.Columns.Select(c => new Column(
                c.Name,
                Enum.Parse<DataType>(c.DataType, true),
                c.IsNullable,
                c.IsPrimaryKey,
                c.IsAutoIncrement
            )).ToList();

            // Load existing indexes
            LoadIndexes();
        }

        /// <summary>
        /// Gets a column by name (case-insensitive)
        /// </summary>
        public Column? GetColumn(string name)
        {
            return Columns.FirstOrDefault(c => 
                string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Gets all column names
        /// </summary>
        public List<string> GetColumnNames()
        {
            return Columns.Select(c => c.Name).ToList();
        }

        /// <summary>
        /// Adds a new row to the table
        /// </summary>
        public int AddRow(Dictionary<string, object?> row)
        {
            // Handle auto-increment columns FIRST (before checking primary key)
            foreach (var column in Columns)
            {
                if (column.IsAutoIncrement)
                {
                    var hasValue = row.TryGetValue(column.Name, out var value);
                    if (!hasValue || value == null)
                    {
                        // Assign auto-increment value
                        if (!_tableData.AutoIncrementValues.ContainsKey(column.Name))
                            _tableData.AutoIncrementValues[column.Name] = 1L;
                        
                        var nextValue = _tableData.AutoIncrementValues[column.Name];
                        row[column.Name] = column.DataType == DataType.Long ? nextValue : (int)nextValue;
                        _tableData.AutoIncrementValues[column.Name] = nextValue + 1;
                    }
                    else
                    {
                        // User provided value, update auto-increment counter if necessary
                        var userValue = Convert.ToInt64(value);
                        if (!_tableData.AutoIncrementValues.ContainsKey(column.Name))
                            _tableData.AutoIncrementValues[column.Name] = userValue + 1;
                        else if (userValue >= _tableData.AutoIncrementValues[column.Name])
                            _tableData.AutoIncrementValues[column.Name] = userValue + 1;
                    }
                }
            }

            // Auto-assign row ID if no primary key exists
            int rowId;
            if (!_tableData.HasPrimaryKey)
            {
                rowId = _tableData.NextRowId++;
                row["__row_id"] = rowId;
            }
            else
            {
                // Use primary key value as row identifier (after auto-increment processing)
                var pkColumn = _tableData.PrimaryKeyColumn!;
                if (!row.ContainsKey(pkColumn) || row[pkColumn] == null)
                    throw new InvalidOperationException($"Primary key '{pkColumn}' cannot be null");
                
                rowId = _tableData.Rows.Count; // Use array index as internal row ID
            }

            // Validate and convert values
            var validatedRow = new Dictionary<string, object?>();
            foreach (var column in Columns)
            {
                var hasValue = row.TryGetValue(column.Name, out var value);
                
                if (!hasValue)
                {
                    if (!column.IsNullable && !column.IsAutoIncrement)
                        throw new InvalidOperationException($"Column '{column.Name}' cannot be NULL");
                    value = null;
                }

                if (value != null && !column.IsValueValid(value))
                    throw new ArgumentException($"Invalid value for column '{column.Name}': {value}");

                validatedRow[column.Name] = value != null ? column.ConvertValue(value) : null;
            }

            // Add auto-generated row ID if no primary key
            if (!_tableData.HasPrimaryKey)
            {
                validatedRow["__row_id"] = rowId;
            }

            _tableData.Rows.Add(validatedRow);

            // Update indexes
            UpdateIndexesForNewRow(validatedRow, _tableData.Rows.Count - 1);

            SaveToFile();
            return rowId;
        }

        /// <summary>
        /// Selects rows based on criteria
        /// </summary>
        public List<Dictionary<string, object?>> SelectRows(
            List<string>? columnNames = null,
            Func<Dictionary<string, object?>, bool>? predicate = null,
            string? orderByColumn = null,
            bool orderDescending = false,
            int? limit = null)
        {
            // Start with all rows or filtered rows
            IEnumerable<Dictionary<string, object?>> query = _tableData.Rows;

            // Apply predicate filtering
            if (predicate != null)
                query = query.Where(predicate);

            // Apply ordering
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                if (orderDescending)
                    query = query.OrderByDescending(row => row.GetValueOrDefault(orderByColumn));
                else
                    query = query.OrderBy(row => row.GetValueOrDefault(orderByColumn));
            }

            // Apply limit
            if (limit.HasValue)
                query = query.Take(limit.Value);

            var result = query.ToList();

            // Select specific columns if specified
            if (columnNames != null && columnNames.Count > 0 && !columnNames.Contains("*"))
            {
                var filteredResult = new List<Dictionary<string, object?>>();
                foreach (var row in result)
                {
                    var filteredRow = new Dictionary<string, object?>();
                    foreach (var columnName in columnNames)
                    {
                        // Skip internal row ID column unless explicitly requested
                        if (columnName == "__row_id" && !columnNames.Contains("__row_id"))
                            continue;

                        if (row.ContainsKey(columnName))
                            filteredRow[columnName] = row[columnName];
                        else if (columnName != "__row_id") // Don't error on missing __row_id
                            throw new ArgumentException($"Column '{columnName}' does not exist");
                    }
                    filteredResult.Add(filteredRow);
                }
                return filteredResult;
            }

            // Remove internal row ID from results unless table has no primary key
            if (_tableData.HasPrimaryKey)
            {
                result = result.Select(row => 
                {
                    var cleanRow = new Dictionary<string, object?>(row);
                    cleanRow.Remove("__row_id");
                    return cleanRow;
                }).ToList();
            }

            return result;
        }

        /// <summary>
        /// Updates rows that match the given predicate
        /// </summary>
        public int UpdateRows(Func<Dictionary<string, object?>, bool> predicate, 
                            string columnName, object? newValue)
        {
            var column = GetColumn(columnName);
            if (column == null)
                throw new ArgumentException($"Column '{columnName}' does not exist");

            if (!column.IsValueValid(newValue))
                throw new ArgumentException($"Invalid value for column '{columnName}': {newValue}");

            var convertedValue = column.ConvertValue(newValue);
            var updatedCount = 0;

            for (int i = 0; i < _tableData.Rows.Count; i++)
            {
                var row = _tableData.Rows[i];
                if (predicate(row))
                {
                    // Update indexes before changing the value
                    UpdateIndexesForUpdatedRow(row, i, columnName, convertedValue);
                    
                    row[columnName] = convertedValue;
                    updatedCount++;
                }
            }

            if (updatedCount > 0)
                SaveToFile();

            return updatedCount;
        }

        /// <summary>
        /// Deletes rows that match the given predicate
        /// </summary>
        public int DeleteRows(Func<Dictionary<string, object?>, bool> predicate)
        {
            var rowsToDelete = new List<int>();
            
            // Find rows to delete (collect indices)
            for (int i = 0; i < _tableData.Rows.Count; i++)
            {
                if (predicate(_tableData.Rows[i]))
                    rowsToDelete.Add(i);
            }

            // Delete rows in reverse order to maintain indices
            for (int i = rowsToDelete.Count - 1; i >= 0; i--)
            {
                var rowIndex = rowsToDelete[i];
                var row = _tableData.Rows[rowIndex];
                
                // Update indexes before removing the row
                UpdateIndexesForDeletedRow(row, rowIndex);
                
                _tableData.Rows.RemoveAt(rowIndex);
            }

            if (rowsToDelete.Count > 0)
            {
                // Rebuild indexes to fix row indices after deletion
                RebuildAllIndexes();
                SaveToFile();
            }

            return rowsToDelete.Count;
        }

        /// <summary>
        /// Creates an index on a column
        /// </summary>
        public void CreateIndex(string columnName)
        {
            var column = GetColumn(columnName);
            if (column == null)
                throw new ArgumentException($"Column '{columnName}' does not exist");

            if (_indexes.ContainsKey(columnName))
                return; // Index already exists

            var index = new Dictionary<object, List<int>>();
            
            // Build index from existing data
            for (int i = 0; i < _tableData.Rows.Count; i++)
            {
                var row = _tableData.Rows[i];
                var value = row.GetValueOrDefault(columnName) ?? DBNull.Value;
                
                if (!index.ContainsKey(value))
                    index[value] = new List<int>();
                
                index[value].Add(i);
            }

            _indexes[columnName] = index;
            _storageManager.SaveIndex(_tableData.TableName, columnName, index);
        }

        /// <summary>
        /// Drops an index on a column
        /// </summary>
        public void DropIndex(string columnName)
        {
            if (_indexes.ContainsKey(columnName))
            {
                _indexes.Remove(columnName);
                _storageManager.DeleteIndex(_tableData.TableName, columnName);
            }
        }

        /// <summary>
        /// Gets row indices for a specific value using index (if available)
        /// </summary>
        public List<int> GetRowIndicesForValue(string columnName, object? value)
        {
            if (_indexes.TryGetValue(columnName, out var index))
            {
                var searchValue = value ?? DBNull.Value;
                return index.GetValueOrDefault(searchValue, new List<int>());
            }

            // Fallback to linear search if no index
            var indices = new List<int>();
            for (int i = 0; i < _tableData.Rows.Count; i++)
            {
                var row = _tableData.Rows[i];
                var rowValue = row.GetValueOrDefault(columnName);
                if (Equals(rowValue, value))
                    indices.Add(i);
            }
            return indices;
        }

        /// <summary>
        /// Gets all index names for this table
        /// </summary>
        public List<string> GetIndexNames()
        {
            return _indexes.Keys.ToList();
        }

        /// <summary>
        /// Saves table data to file
        /// </summary>
        private void SaveToFile()
        {
            _storageManager.SaveTable(_tableData.TableName, _tableData);
        }

        /// <summary>
        /// Loads all indexes from files
        /// </summary>
        private void LoadIndexes()
        {
            var indexNames = _storageManager.GetIndexNames(_tableData.TableName);
            foreach (var indexName in indexNames)
            {
                var indexData = _storageManager.LoadIndex(_tableData.TableName, indexName);
                if (indexData != null)
                    _indexes[indexName] = indexData;
            }
        }

        /// <summary>
        /// Updates indexes when a new row is added
        /// </summary>
        private void UpdateIndexesForNewRow(Dictionary<string, object?> row, int rowIndex)
        {
            foreach (var kvp in _indexes)
            {
                var columnName = kvp.Key;
                var index = kvp.Value;
                var value = row.GetValueOrDefault(columnName) ?? DBNull.Value;
                
                if (!index.ContainsKey(value))
                    index[value] = new List<int>();
                
                index[value].Add(rowIndex);
                _storageManager.SaveIndex(_tableData.TableName, columnName, index);
            }
        }

        /// <summary>
        /// Updates indexes when a row is updated
        /// </summary>
        private void UpdateIndexesForUpdatedRow(Dictionary<string, object?> row, int rowIndex, 
                                              string updatedColumn, object? newValue)
        {
            if (_indexes.TryGetValue(updatedColumn, out var index))
            {
                // Remove old value from index
                var oldValue = row.GetValueOrDefault(updatedColumn) ?? DBNull.Value;
                if (index.ContainsKey(oldValue))
                {
                    index[oldValue].Remove(rowIndex);
                    if (index[oldValue].Count == 0)
                        index.Remove(oldValue);
                }

                // Add new value to index
                var searchValue = newValue ?? DBNull.Value;
                if (!index.ContainsKey(searchValue))
                    index[searchValue] = new List<int>();
                
                index[searchValue].Add(rowIndex);
                _storageManager.SaveIndex(_tableData.TableName, updatedColumn, index);
            }
        }

        /// <summary>
        /// Updates indexes when a row is deleted
        /// </summary>
        private void UpdateIndexesForDeletedRow(Dictionary<string, object?> row, int rowIndex)
        {
            foreach (var kvp in _indexes)
            {
                var columnName = kvp.Key;
                var index = kvp.Value;
                var value = row.GetValueOrDefault(columnName) ?? DBNull.Value;
                
                if (index.ContainsKey(value))
                {
                    index[value].Remove(rowIndex);
                    if (index[value].Count == 0)
                        index.Remove(value);
                }
            }
        }

        /// <summary>
        /// Rebuilds all indexes after row deletion to fix indices
        /// </summary>
        private void RebuildAllIndexes()
        {
            foreach (var columnName in _indexes.Keys.ToList())
            {
                var index = new Dictionary<object, List<int>>();
                
                for (int i = 0; i < _tableData.Rows.Count; i++)
                {
                    var row = _tableData.Rows[i];
                    var value = row.GetValueOrDefault(columnName) ?? DBNull.Value;
                    
                    if (!index.ContainsKey(value))
                        index[value] = new List<int>();
                    
                    index[value].Add(i);
                }

                _indexes[columnName] = index;
                _storageManager.SaveIndex(_tableData.TableName, columnName, index);
            }
        }
    }
}
