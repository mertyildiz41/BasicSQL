using System;
using System.Collections.Generic;
using System.Linq;

namespace BasicSQL.Models
{
    /// <summary>
    /// Represents a database table with columns and rows
    /// </summary>
    public class Table
    {
        public string Name { get; set; } = string.Empty;
        public List<Column> Columns { get; set; } = new List<Column>();
        public List<Dictionary<string, object?>> Rows { get; set; } = new List<Dictionary<string, object?>>();

        public Table() { }

        public Table(string name)
        {
            Name = name;
        }

        public Table(string name, List<Column> columns)
        {
            Name = name;
            Columns = columns;
        }

        /// <summary>
        /// Gets all column names
        /// </summary>
        public List<string> GetColumnNames()
        {
            return Columns.Select(c => c.Name).ToList();
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
        /// Validates and adds a new row to the table
        /// </summary>
        public void AddRow(Dictionary<string, object?> row)
        {
            var validatedRow = new Dictionary<string, object?>();

            // Validate each column
            foreach (var column in Columns)
            {
                var hasValue = row.TryGetValue(column.Name, out var value);
                
                if (!hasValue)
                {
                    if (!column.IsNullable)
                        throw new InvalidOperationException($"Column '{column.Name}' cannot be NULL");
                    value = null;
                }

                if (!column.IsValueValid(value))
                    throw new ArgumentException($"Invalid value for column '{column.Name}': {value}");

                validatedRow[column.Name] = column.ConvertValue(value);
            }

            Rows.Add(validatedRow);
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

            foreach (var row in Rows.Where(predicate))
            {
                row[columnName] = convertedValue;
                updatedCount++;
            }

            return updatedCount;
        }

        /// <summary>
        /// Deletes rows that match the given predicate
        /// </summary>
        public int DeleteRows(Func<Dictionary<string, object?>, bool> predicate)
        {
            var rowsToDelete = Rows.Where(predicate).ToList();
            foreach (var row in rowsToDelete)
            {
                Rows.Remove(row);
            }
            return rowsToDelete.Count;
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
            var query = predicate != null ? Rows.Where(predicate) : Rows;

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
                        if (row.ContainsKey(columnName))
                            filteredRow[columnName] = row[columnName];
                        else
                            throw new ArgumentException($"Column '{columnName}' does not exist");
                    }
                    filteredResult.Add(filteredRow);
                }
                return filteredResult;
            }

            return result;
        }

        public override string ToString()
        {
            return $"Table '{Name}' ({Columns.Count} columns, {Rows.Count} rows)";
        }
    }
}
