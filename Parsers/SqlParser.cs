using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using BasicSQL.Models;

namespace BasicSQL.Parsers
{
    /// <summary>
    /// Parses SQL statements and extracts relevant information
    /// </summary>
    public static class SqlParser
    {
        /// <summary>
        /// Parses a CREATE TABLE statement
        /// </summary>
        public static (string tableName, List<Column> columns) ParseCreateTable(string sql)
        {
            var pattern = @"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)";
            var match = Regex.Match(sql, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
                throw new ArgumentException("Invalid CREATE TABLE syntax");

            var tableName = match.Groups[1].Value;
            var columnsString = match.Groups[2].Value;

            var columns = ParseColumnDefinitions(columnsString);
            return (tableName, columns);
        }

        /// <summary>
        /// Parses column definitions from CREATE TABLE statement
        /// </summary>
        private static List<Column> ParseColumnDefinitions(string columnsString)
        {
            var columns = new List<Column>();
            var columnDefinitions = SplitColumnDefinitions(columnsString);

            foreach (var columnDef in columnDefinitions)
            {
                var parts = columnDef.Trim().Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2)
                    throw new ArgumentException($"Invalid column definition: {columnDef}");

                var columnName = parts[0];
                var dataTypeString = parts[1].ToUpper();

                if (!Enum.TryParse<DataType>(dataTypeString, true, out var dataType))
                    throw new ArgumentException($"Unsupported data type: {dataTypeString}");

                var isNullable = true;
                var isPrimaryKey = false;
                var isAutoIncrement = false;

                // Check for constraints
                var upperParts = parts.Select(p => p.ToUpper()).ToArray();
                
                if (ContainsSequence(upperParts, new[] { "NOT", "NULL" }))
                    isNullable = false;

                if (ContainsSequence(upperParts, new[] { "PRIMARY", "KEY" }))
                {
                    isPrimaryKey = true;
                    isNullable = false;
                }

                if (upperParts.Contains("AUTO_INCREMENT") || upperParts.Contains("AUTOINCREMENT"))
                {
                    isAutoIncrement = true;
                    isNullable = false;
                }

                columns.Add(new Column(columnName, dataType, isNullable, isPrimaryKey, isAutoIncrement));
            }

            return columns;
        }

        /// <summary>
        /// Splits column definitions considering parentheses and commas
        /// </summary>
        private static List<string> SplitColumnDefinitions(string columnsString)
        {
            var definitions = new List<string>();
            var current = "";
            var parenthesesLevel = 0;

            foreach (var ch in columnsString)
            {
                if (ch == '(')
                    parenthesesLevel++;
                else if (ch == ')')
                    parenthesesLevel--;
                else if (ch == ',' && parenthesesLevel == 0)
                {
                    definitions.Add(current.Trim());
                    current = "";
                    continue;
                }

                current += ch;
            }

            if (!string.IsNullOrWhiteSpace(current))
                definitions.Add(current.Trim());

            return definitions;
        }

        /// <summary>
        /// Parses an INSERT INTO statement
        /// </summary>
        public static (string tableName, List<string>? columns, List<object?> values) ParseInsert(string sql)
        {
            var pattern = @"INSERT\s+INTO\s+(\w+)\s*(?:\((.*?)\))?\s*VALUES\s*\((.*)\)";
            var match = Regex.Match(sql, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
                throw new ArgumentException("Invalid INSERT syntax");

            var tableName = match.Groups[1].Value;
            var columnsString = match.Groups[2].Success ? match.Groups[2].Value : null;
            var valuesString = match.Groups[3].Value;

            List<string>? columns = null;
            if (!string.IsNullOrEmpty(columnsString))
            {
                columns = columnsString.Split(',')
                    .Select(c => c.Trim())
                    .ToList();
            }

            var values = ParseValues(valuesString);
            return (tableName, columns, values);
        }

        /// <summary>
        /// Parses a SELECT statement
        /// </summary>
        public static SelectQuery ParseSelect(string sql)
        {
            var query = new SelectQuery();

            // Extract table name
            var fromMatch = Regex.Match(sql, @"FROM\s+(\w+)", RegexOptions.IgnoreCase);
            if (!fromMatch.Success)
                throw new ArgumentException("Invalid SELECT syntax - missing FROM clause");
            query.TableName = fromMatch.Groups[1].Value;

            // Extract columns
            var selectMatch = Regex.Match(sql, @"SELECT\s+(.*?)\s+FROM", RegexOptions.IgnoreCase);
            if (!selectMatch.Success)
                throw new ArgumentException("Invalid SELECT syntax");

            var columnsString = selectMatch.Groups[1].Value.Trim();
            if (columnsString == "*")
                query.Columns = new List<string> { "*" };
            else
                query.Columns = columnsString.Split(',').Select(c => c.Trim()).ToList();

            // Extract WHERE clause
            var whereMatch = Regex.Match(sql, @"WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|$)", RegexOptions.IgnoreCase);
            if (whereMatch.Success)
                query.WhereClause = whereMatch.Groups[1].Value.Trim();

            // Extract ORDER BY clause
            var orderMatch = Regex.Match(sql, @"ORDER\s+BY\s+(.*?)(?:\s+LIMIT|$)", RegexOptions.IgnoreCase);
            if (orderMatch.Success)
            {
                var orderClause = orderMatch.Groups[1].Value.Trim();
                var orderParts = orderClause.Split(' ');
                query.OrderByColumn = orderParts[0];
                query.OrderDescending = orderParts.Length > 1 && 
                    string.Equals(orderParts[1], "DESC", StringComparison.OrdinalIgnoreCase);
            }

            // Extract LIMIT clause
            var limitMatch = Regex.Match(sql, @"LIMIT\s+(\d+)", RegexOptions.IgnoreCase);
            if (limitMatch.Success)
                query.Limit = int.Parse(limitMatch.Groups[1].Value);

            return query;
        }

        /// <summary>
        /// Parses an UPDATE statement
        /// </summary>
        public static (string tableName, string columnName, object? value, string? whereClause) ParseUpdate(string sql)
        {
            var pattern = @"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*?))?$";
            var match = Regex.Match(sql, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
                throw new ArgumentException("Invalid UPDATE syntax");

            var tableName = match.Groups[1].Value;
            var setClause = match.Groups[2].Value;
            var whereClause = match.Groups[3].Success ? match.Groups[3].Value : null;

            // Parse SET clause (simplified - assumes single column update)
            var setParts = setClause.Split('=');
            if (setParts.Length != 2)
                throw new ArgumentException("Invalid SET clause");

            var columnName = setParts[0].Trim();
            var valueString = setParts[1].Trim();
            var value = ParseSingleValue(valueString);

            return (tableName, columnName, value, whereClause);
        }

        /// <summary>
        /// Parses an UPDATE statement supporting multiple columns
        /// </summary>
        public static (string tableName, Dictionary<string, object?> updates, string? whereClause) ParseUpdateMultipleColumns(string sql)
        {
            var pattern = @"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*?))?$";
            var match = Regex.Match(sql, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
                throw new ArgumentException("Invalid UPDATE syntax");

            var tableName = match.Groups[1].Value;
            var setClause = match.Groups[2].Value;
            var whereClause = match.Groups[3].Success ? match.Groups[3].Value : null;

            // Parse SET clause - handles multiple column=value pairs
            var updates = new Dictionary<string, object?>();
            var assignments = SplitSetClause(setClause);

            foreach (var assignment in assignments)
            {
                var setParts = assignment.Split('=');
                if (setParts.Length != 2)
                    throw new ArgumentException($"Invalid SET assignment: {assignment}");

                var columnName = setParts[0].Trim();
                var valueString = setParts[1].Trim();
                var value = ParseSingleValue(valueString);
                updates[columnName] = value;
            }

            return (tableName, updates, whereClause);
        }

        /// <summary>
        /// Splits SET clause into individual column=value assignments
        /// Handles quoted strings properly
        /// </summary>
        private static List<string> SplitSetClause(string setClause)
        {
            var assignments = new List<string>();
            var current = "";
            var inQuotes = false;

            for (int i = 0; i < setClause.Length; i++)
            {
                var ch = setClause[i];

                if (ch == '\'' && (i == 0 || setClause[i - 1] != '\\'))
                {
                    inQuotes = !inQuotes;
                    current += ch;
                }
                else if (ch == ',' && !inQuotes)
                {
                    assignments.Add(current.Trim());
                    current = "";
                }
                else
                {
                    current += ch;
                }
            }

            if (!string.IsNullOrWhiteSpace(current))
                assignments.Add(current.Trim());

            return assignments;
        }

        /// <summary>
        /// Parses a DELETE statement
        /// </summary>
        public static (string tableName, string? whereClause) ParseDelete(string sql)
        {
            var pattern = @"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*?))?$";
            var match = Regex.Match(sql, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
                throw new ArgumentException("Invalid DELETE syntax");

            var tableName = match.Groups[1].Value;
            var whereClause = match.Groups[2].Success ? match.Groups[2].Value : null;

            return (tableName, whereClause);
        }

        /// <summary>
        /// Parses values from VALUES clause
        /// </summary>
        private static List<object?> ParseValues(string valuesString)
        {
            var values = new List<object?>();
            var parts = SplitValues(valuesString);

            foreach (var part in parts)
            {
                values.Add(ParseSingleValue(part));
            }

            return values;
        }

        /// <summary>
        /// Parses a single value (string, number, or null)
        /// </summary>
        private static object? ParseSingleValue(string valueString)
        {
            var trimmed = valueString.Trim();

            if (string.Equals(trimmed, "NULL", StringComparison.OrdinalIgnoreCase))
                return null;

            if (trimmed.StartsWith("'") && trimmed.EndsWith("'") && trimmed.Length >= 2)
                return trimmed.Substring(1, trimmed.Length - 2);

            if (int.TryParse(trimmed, out var intValue))
                return intValue;

            if (double.TryParse(trimmed, out var doubleValue))
                return doubleValue;

            return trimmed;
        }

        /// <summary>
        /// Splits values considering quotes and commas
        /// </summary>
        private static List<string> SplitValues(string valuesString)
        {
            var values = new List<string>();
            var current = "";
            var inQuotes = false;

            for (int i = 0; i < valuesString.Length; i++)
            {
                var ch = valuesString[i];

                if (ch == '\'' && (i == 0 || valuesString[i - 1] != '\\'))
                {
                    inQuotes = !inQuotes;
                    current += ch;
                }
                else if (ch == ',' && !inQuotes)
                {
                    values.Add(current.Trim());
                    current = "";
                }
                else
                {
                    current += ch;
                }
            }

            if (!string.IsNullOrWhiteSpace(current))
                values.Add(current.Trim());

            return values;
        }

        /// <summary>
        /// Creates a predicate function from a WHERE clause
        /// </summary>
        public static Func<Dictionary<string, object?>, bool> ParseWhereClause(string whereClause)
        {
            // Simple WHERE clause parsing (column operator value)
            var pattern = @"(\w+)\s*(=|!=|<>|<|>|<=|>=)\s*(.*)";
            var match = Regex.Match(whereClause, pattern);

            if (!match.Success)
                return _ => true; // If we can't parse, return all rows

            var columnName = match.Groups[1].Value;
            var operatorStr = match.Groups[2].Value;
            var valueString = match.Groups[3].Value;
            var value = ParseSingleValue(valueString);

            return row =>
            {
                if (!row.TryGetValue(columnName, out var rowValue))
                    return false;

                return operatorStr switch
                {
                    "=" => Equals(rowValue, value),
                    "!=" or "<>" => !Equals(rowValue, value),
                    "<" => CompareValues(rowValue, value) < 0,
                    ">" => CompareValues(rowValue, value) > 0,
                    "<=" => CompareValues(rowValue, value) <= 0,
                    ">=" => CompareValues(rowValue, value) >= 0,
                    _ => true
                };
            };
        }

        /// <summary>
        /// Compares two values for ordering operations
        /// </summary>
        private static int CompareValues(object? left, object? right)
        {
            if (left == null && right == null) return 0;
            if (left == null) return -1;
            if (right == null) return 1;

            if (left is IComparable leftComparable && right is IComparable)
            {
                try
                {
                    return leftComparable.CompareTo(Convert.ChangeType(right, left.GetType()));
                }
                catch
                {
                    return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
                }
            }

            return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Checks if an array contains a sequence of elements
        /// </summary>
        private static bool ContainsSequence<T>(T[] array, T[] sequence)
        {
            for (int i = 0; i <= array.Length - sequence.Length; i++)
            {
                bool found = true;
                for (int j = 0; j < sequence.Length; j++)
                {
                    if (!EqualityComparer<T>.Default.Equals(array[i + j], sequence[j]))
                    {
                        found = false;
                        break;
                    }
                }
                if (found) return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Represents a parsed SELECT query
    /// </summary>
    public class SelectQuery
    {
        public string TableName { get; set; } = string.Empty;
        public List<string> Columns { get; set; } = new List<string>();
        public string? WhereClause { get; set; }
        public string? OrderByColumn { get; set; }
        public bool OrderDescending { get; set; }
        public int? Limit { get; set; }
    }
}
