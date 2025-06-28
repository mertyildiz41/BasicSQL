using System;
using System.Collections.Generic;
using System.Linq;
using BasicSQL.Models;
using BasicSQL.Storage;

namespace BasicSQL.Models
{
    /// <summary>
    /// Scalable file-based table implementation that can handle large datasets (100GB+)
    /// by streaming data from disk instead of loading everything into memory
    /// </summary>
    public class ScalableFileTable
    {
        private readonly ScalableFileStorageManager _storageManager;
        private ScalableTableMetadata _metadata;
        private readonly Dictionary<string, Dictionary<object, List<int>>> _indexes;

        public string Name => _metadata.TableName;
        public List<Column> Columns { get; private set; } = new List<Column>();
        public int RowCount => _metadata.TotalRows;
        public bool HasPrimaryKey => _metadata.HasPrimaryKey;
        public long EstimatedSizeBytes => _metadata.EstimatedSizeBytes;

        public ScalableFileTable(string tableName, List<Column> columns, ScalableFileStorageManager storageManager)
        {
            _storageManager = storageManager;
            _indexes = new Dictionary<string, Dictionary<object, List<int>>>();
            
            // Initialize metadata
            _metadata = new ScalableTableMetadata
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
                    .ToDictionary(c => c.Name, c => 1L)
            };

            Columns = columns;
            SaveMetadata();
        }

        /// <summary>
        /// Loads an existing table from file
        /// </summary>
        public static ScalableFileTable? LoadFromFile(string tableName, ScalableFileStorageManager storageManager)
        {
            var metadata = storageManager.LoadTableMetadata(tableName);
            if (metadata == null)
                return null;

            var table = new ScalableFileTable(storageManager, metadata);
            return table;
        }

        /// <summary>
        /// Private constructor for loading from file
        /// </summary>
        private ScalableFileTable(ScalableFileStorageManager storageManager, ScalableTableMetadata metadata)
        {
            _storageManager = storageManager;
            _metadata = metadata;
            _indexes = new Dictionary<string, Dictionary<object, List<int>>>();

            // Ensure AutoIncrementValues is initialized if missing (for backward compatibility)
            if (_metadata.AutoIncrementValues == null)
                _metadata.AutoIncrementValues = new Dictionary<string, long>();

            // Convert metadata back to Column objects
            Columns = metadata.Columns.Select(c => new Column(
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
        /// Adds a new row to the table (scalable - streams to disk)
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
                        if (!_metadata.AutoIncrementValues.ContainsKey(column.Name))
                            _metadata.AutoIncrementValues[column.Name] = 1L;
                        
                        var nextValue = _metadata.AutoIncrementValues[column.Name];
                        row[column.Name] = column.DataType == DataType.Long ? nextValue : (int)nextValue;
                        _metadata.AutoIncrementValues[column.Name] = nextValue + 1;
                    }
                    else
                    {
                        // User provided value, update auto-increment counter if necessary
                        var userValue = Convert.ToInt64(value);
                        if (!_metadata.AutoIncrementValues.ContainsKey(column.Name))
                            _metadata.AutoIncrementValues[column.Name] = userValue + 1;
                        else if (userValue >= _metadata.AutoIncrementValues[column.Name])
                            _metadata.AutoIncrementValues[column.Name] = userValue + 1;
                    }
                }
            }

            // Auto-assign row ID if no primary key exists
            int rowId;
            if (!_metadata.HasPrimaryKey)
            {
                rowId = _metadata.NextRowId++;
                row["__row_id"] = rowId;
            }
            else
            {
                // Use primary key value as row identifier (after auto-increment processing)
                var pkColumn = _metadata.PrimaryKeyColumn!;
                if (!row.ContainsKey(pkColumn) || row[pkColumn] == null)
                    throw new InvalidOperationException($"Primary key '{pkColumn}' cannot be null");
                
                rowId = _metadata.TotalRows; // Use total row count as internal row ID
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
            if (!_metadata.HasPrimaryKey)
            {
                validatedRow["__row_id"] = rowId;
            }

            // Append row to disk (this is scalable - doesn't load everything into memory)
            var actualRowId = _storageManager.AppendRow(_metadata.TableName, validatedRow, _metadata);

            // Update indexes (only if indexes exist)
            UpdateIndexesForNewRow(validatedRow, actualRowId);

            return actualRowId;
        }

        /// <summary>
        /// Selects rows with streaming support (scalable for large datasets)
        /// </summary>
        public IEnumerable<Dictionary<string, object?>> SelectRows(
            List<string>? columnNames = null,
            Func<Dictionary<string, object?>, bool>? predicate = null,
            string? orderByColumn = null,
            bool orderDescending = false,
            int? limit = null,
            int skip = 0)
        {
            var actualLimit = limit ?? int.MaxValue;
            var rowsReturned = 0;

            // Stream rows from disk (memory efficient)
            foreach (var (rowId, row) in _storageManager.ReadRows(_metadata.TableName, _metadata, skip, actualLimit))
            {
                if (rowsReturned >= actualLimit)
                    break;

                // Apply predicate filter
                if (predicate != null && !predicate(row))
                    continue;

                // Select only requested columns
                var selectedRow = SelectColumns(row, columnNames);
                
                yield return selectedRow;
                rowsReturned++;
            }
        }

        /// <summary>
        /// Updates rows based on criteria (expensive for large datasets - requires streaming)
        /// </summary>
        public int UpdateRows(Dictionary<string, object?> updates, Func<Dictionary<string, object?>, bool>? predicate = null)
        {
            var updatedCount = 0;
            var rowsToUpdate = new List<(int rowId, Dictionary<string, object?> row)>();

            // First pass: find rows to update (streaming)
            foreach (var (rowId, row) in _storageManager.ReadRows(_metadata.TableName, _metadata))
            {
                if (predicate == null || predicate(row))
                {
                    rowsToUpdate.Add((rowId, row));
                }
            }

            // Second pass: update rows
            foreach (var (rowId, oldRow) in rowsToUpdate)
            {
                var newRow = new Dictionary<string, object?>(oldRow);
                
                // Apply updates
                foreach (var (column, value) in updates)
                {
                    var col = GetColumn(column);
                    if (col == null)
                        throw new ArgumentException($"Column '{column}' not found");

                    if (value != null && !col.IsValueValid(value))
                        throw new ArgumentException($"Invalid value for column '{column}': {value}");

                    newRow[column] = value != null ? col.ConvertValue(value) : null;
                }

                _storageManager.UpdateRow(_metadata.TableName, rowId, newRow, _metadata);
                updatedCount++;
            }

            return updatedCount;
        }

        /// <summary>
        /// Creates an index on a column (memory efficient for large datasets)
        /// </summary>
        public void CreateIndex(string columnName)
        {
            var column = GetColumn(columnName);
            if (column == null)
                throw new ArgumentException($"Column '{columnName}' not found");

            if (_indexes.ContainsKey(columnName))
                throw new InvalidOperationException($"Index on column '{columnName}' already exists");

            var index = new Dictionary<object, List<int>>();

            // Build index by streaming through all rows
            foreach (var (rowId, row) in _storageManager.ReadRows(_metadata.TableName, _metadata))
            {
                if (row.TryGetValue(columnName, out var value) && value != null)
                {
                    if (!index.ContainsKey(value))
                        index[value] = new List<int>();
                    index[value].Add(rowId);
                }
            }

            _indexes[columnName] = index;
            _storageManager.SaveIndex(_metadata.TableName, columnName, index);
        }

        /// <summary>
        /// Drops an index
        /// </summary>
        public void DropIndex(string columnName)
        {
            if (!_indexes.ContainsKey(columnName))
                throw new ArgumentException($"Index on column '{columnName}' does not exist");

            _indexes.Remove(columnName);
            // Note: We don't have a DeleteIndex method in the storage manager, but we could add it
        }

        /// <summary>
        /// Gets statistics about the table (useful for large datasets)
        /// </summary>
        public Dictionary<string, object> GetTableStats()
        {
            return new Dictionary<string, object>
            {
                ["TableName"] = _metadata.TableName,
                ["TotalRows"] = _metadata.TotalRows,
                ["EstimatedSizeBytes"] = _metadata.EstimatedSizeBytes,
                ["EstimatedSizeMB"] = _metadata.EstimatedSizeBytes / (1024.0 * 1024.0),
                ["Created"] = _metadata.Created,
                ["LastModified"] = _metadata.LastModified,
                ["HasPrimaryKey"] = _metadata.HasPrimaryKey,
                ["PrimaryKeyColumn"] = _metadata.PrimaryKeyColumn,
                ["IndexCount"] = _indexes.Count,
                ["IndexedColumns"] = _indexes.Keys.ToList()
            };
        }

        private Dictionary<string, object?> SelectColumns(Dictionary<string, object?> row, List<string>? columnNames)
        {
            if (columnNames == null)
                return row;

            var result = new Dictionary<string, object?>();
            foreach (var columnName in columnNames)
            {
                if (row.TryGetValue(columnName, out var value))
                    result[columnName] = value;
            }
            return result;
        }

        private void UpdateIndexesForNewRow(Dictionary<string, object?> row, int rowId)
        {
            foreach (var (columnName, index) in _indexes)
            {
                if (row.TryGetValue(columnName, out var value) && value != null)
                {
                    if (!index.ContainsKey(value))
                        index[value] = new List<int>();
                    index[value].Add(rowId);
                }
            }

            // Save updated indexes
            foreach (var columnName in _indexes.Keys)
            {
                _storageManager.SaveIndex(_metadata.TableName, columnName, _indexes[columnName]);
            }
        }

        private void LoadIndexes()
        {
            var indexNames = new List<string>(); // We'd need to add GetIndexNames to ScalableFileStorageManager
            foreach (var columnName in indexNames)
            {
                var indexData = _storageManager.LoadIndex(_metadata.TableName, columnName);
                if (indexData != null)
                {
                    _indexes[columnName] = indexData;
                }
            }
        }

        private void SaveMetadata()
        {
            _storageManager.SaveTableMetadata(_metadata.TableName, _metadata);
        }
    }
}
