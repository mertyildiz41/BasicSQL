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
        public static (string tableName, List<Column> columns, bool ifNotExists) ParseCreateTable(string sql)
        {
            var pattern = @"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*)\)";
            var match = Regex.Match(sql, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
                throw new ArgumentException("Invalid CREATE TABLE syntax");

            var ifNotExists = match.Groups[1].Success;
            var tableName = match.Groups[2].Value;
            var columnsString = match.Groups[3].Value;

            var columns = ParseColumnDefinitions(columnsString);
            return (tableName, columns, ifNotExists);
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

            // Simplified parsing logic for clauses
            string remainingSql = sql;

            // 1. SELECT
            var selectMatch = Regex.Match(remainingSql, @"^SELECT\s+(.*?)\s+FROM", RegexOptions.IgnoreCase);
            if (!selectMatch.Success) throw new ArgumentException("Invalid SELECT syntax");
            var columnsString = selectMatch.Groups[1].Value.Trim();
            query.Columns = columnsString == "*" ? new List<string> { "*" } : SplitValues(columnsString);
            remainingSql = remainingSql.Substring(selectMatch.Length);

            // 2. FROM
            var trimmedSql = remainingSql.Trim();
            var fromMatch = Regex.Match(trimmedSql, @"^([\w_]+)", RegexOptions.IgnoreCase);
            if (!fromMatch.Success) throw new ArgumentException("Missing table name in FROM clause");
            query.TableName = fromMatch.Groups[1].Value;
            remainingSql = trimmedSql.Substring(fromMatch.Length).Trim();

            // 3. JOINs
            var joinPattern = @"^(INNER|LEFT)\s+JOIN\s+(\w+)\s+ON\s+([\w\.]+)\s*=\s*([\w\.]+)((?:\s+WHERE|\s+ORDER\s+BY|\s+LIMIT)|$)";
            Match joinMatch;
            while ((joinMatch = Regex.Match(remainingSql, joinPattern, RegexOptions.IgnoreCase)).Success)
            {
                var joinClause = new JoinClause
                {
                    JoinType = "LEFT".Equals(joinMatch.Groups[1].Value, StringComparison.OrdinalIgnoreCase) ? JoinType.Left : JoinType.Inner,
                    ToTableName = joinMatch.Groups[2].Value,
                    OnClause = $"{joinMatch.Groups[3].Value} = {joinMatch.Groups[4].Value}",
                    ParsedOnClause = (joinMatch.Groups[3].Value, joinMatch.Groups[4].Value)
                };
                query.Joins.Add(joinClause);

                // Remove the parsed JOIN clause from the string
                var joinClauseLength = joinMatch.Length - joinMatch.Groups[5].Value.Length;
                remainingSql = remainingSql.Substring(joinClauseLength).Trim();
            }

            // 4. WHERE
            var whereMatch = Regex.Match(remainingSql, @"^WHERE\s+(.*?)(?:\s+ORDER\s+BY|\s+LIMIT|$)", RegexOptions.IgnoreCase);
            if (whereMatch.Success)
            {
                query.WhereClause = whereMatch.Groups[1].Value.Trim();
            }

            // 5. ORDER BY
            var orderMatch = Regex.Match(sql, @"ORDER\s+BY\s+(.*?)(?:\s+LIMIT|$)", RegexOptions.IgnoreCase);
            if (orderMatch.Success)
            {
                var orderClause = orderMatch.Groups[1].Value.Trim();
                var orderParts = orderClause.Split(' ');
                query.OrderByColumn = orderParts[0];
                query.OrderDescending = orderParts.Length > 1 &&
                    string.Equals(orderParts[1], "DESC", StringComparison.OrdinalIgnoreCase);
            }

            // 6. LIMIT
            var limitMatch = Regex.Match(sql, @"LIMIT\s+(\d+)", RegexOptions.IgnoreCase);
            if (limitMatch.Success)
            {
                query.Limit = int.Parse(limitMatch.Groups[1].Value);
            }

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
        /// Parses a single value (string, number, datetime, decimal, or null)
        /// </summary>
        private static object? ParseSingleValue(string valueString)
        {
            var trimmed = valueString.Trim();

            if (string.Equals(trimmed, "NULL", StringComparison.OrdinalIgnoreCase))
                return null;

            // Handle quoted strings
            if (trimmed.StartsWith("'") && trimmed.EndsWith("'") && trimmed.Length >= 2)
            {
                var stringValue = trimmed.Substring(1, trimmed.Length - 2);
                
                // Try to parse as DateTime first (common format: 'YYYY-MM-DD HH:MM:SS')
                if (DateTime.TryParse(stringValue, out var dateTimeValue))
                    return dateTimeValue;
                
                // Try to parse as decimal if it looks numeric
                if (decimal.TryParse(stringValue, out var quotedDecimalValue))
                    return quotedDecimalValue;
                
                // Return as string
                return stringValue;
            }

            // Try to parse unquoted numeric values in order of specificity
            // Try int first (most specific)
            if (int.TryParse(trimmed, out var intValue))
                return intValue;

            // Try long next
            if (long.TryParse(trimmed, out var longValue))
                return longValue;

            // Try decimal (more specific than double)
            if (decimal.TryParse(trimmed, out var decimalValue))
                return decimalValue;

            // Try double
            if (double.TryParse(trimmed, out var doubleValue))
                return doubleValue;

            // Try to parse as DateTime for unquoted values (less common but possible)
            if (DateTime.TryParse(trimmed, out var unquotedDateTimeValue))
                return unquotedDateTimeValue;

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
            whereClause = whereClause.Trim();

            // Handle LEN function
            if (whereClause.StartsWith("LEN(", StringComparison.OrdinalIgnoreCase))
            {
                return ParseLenFunction(whereClause);
            }

            // Handle simple column comparisons
            return ParseSimpleComparison(whereClause);
        }

        private static Func<Dictionary<string, object?>, bool> ParseLenFunction(string whereClause)
        {
            // Extract LEN(column) operator value
            var match = Regex.Match(whereClause, @"LEN\s*\(\s*([\w_]+)\s*\)\s*(=|!=|<>|>|<|>=|<=)\s*(.+)", RegexOptions.IgnoreCase);
            if (!match.Success) return _ => true;

            var columnName = match.Groups[1].Value;
            var op = match.Groups[2].Value;
            var valueStr = match.Groups[3].Value.Trim();
            var targetValue = ParseSingleValue(valueStr);

            return row =>
            {
                if (!row.TryGetValue(columnName, out var columnValue)) return false;
                var length = columnValue?.ToString()?.Length ?? 0;

                return op.ToUpper() switch
                {
                    "=" => length.Equals(targetValue),
                    "!=" or "<>" => !length.Equals(targetValue),
                    ">" => length > Convert.ToInt32(targetValue),
                    "<" => length < Convert.ToInt32(targetValue),
                    ">=" => length >= Convert.ToInt32(targetValue),
                    "<=" => length <= Convert.ToInt32(targetValue),
                    _ => true
                };
            };
        }

        private static Func<Dictionary<string, object?>, bool> ParseSimpleComparison(string whereClause)
        {
            // Handle simple column = value, column != value, etc.
            var match = Regex.Match(whereClause, @"([\w_]+)\s*(=|!=|<>|>|<|>=|<=)\s*(.+)", RegexOptions.IgnoreCase);
            if (!match.Success) return _ => true;

            var columnName = match.Groups[1].Value;
            var op = match.Groups[2].Value;
            var valueStr = match.Groups[3].Value.Trim();
            var targetValue = ParseSingleValue(valueStr);

            return row =>
            {
                if (!row.TryGetValue(columnName, out var columnValue)) return false;

                return op.ToUpper() switch
                {
                    "=" => AreEqual(columnValue, targetValue),
                    "!=" or "<>" => !AreEqual(columnValue, targetValue),
                    ">" => CompareValues(columnValue, targetValue) > 0,
                    "<" => CompareValues(columnValue, targetValue) < 0,
                    ">=" => CompareValues(columnValue, targetValue) >= 0,
                    "<=" => CompareValues(columnValue, targetValue) <= 0,
                    _ => true
                };
            };
        }

        private static bool AreEqual(object? left, object? right)
        {
            if (left == null && right == null) return true;
            if (left == null || right == null) return false;

            // Handle numeric equality more carefully
            if (IsNumeric(left) && IsNumeric(right))
            {
                try
                {
                    var leftDecimal = Convert.ToDecimal(left);
                    var rightDecimal = Convert.ToDecimal(right);
                    return leftDecimal == rightDecimal;
                }
                catch
                {
                    // Fall through to object.Equals
                }
            }

            // For strings and other types, use direct comparison
            return object.Equals(left, right) || string.Equals(left?.ToString(), right?.ToString(), StringComparison.OrdinalIgnoreCase);
        }


        /// <summary>
        /// Compares two values for ordering operations
        /// </summary>
        private static int CompareValues(object? left, object? right)
        {
            if (left == null && right == null) return 0;
            if (left == null) return -1;
            if (right == null) return 1;

            // Handle numeric comparisons more robustly
            if (IsNumeric(left) && IsNumeric(right))
            {
                try
                {
                    var leftDecimal = Convert.ToDecimal(left);
                    var rightDecimal = Convert.ToDecimal(right);
                    return leftDecimal.CompareTo(rightDecimal);
                }
                catch
                {
                    // Fallback for types that can't be converted to decimal
                }
            }

            if (left is IComparable leftComparable)
            {
                try
                {
                    var convertedRight = Convert.ChangeType(right, left.GetType());
                    return leftComparable.CompareTo(convertedRight);
                }
                catch
                {
                    // Fallback to string comparison if types are not compatible
                }
            }

            return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsNumeric(object? value)
        {
            if (value == null) return false;
            return value is sbyte
                || value is byte
                || value is short
                || value is ushort
                || value is int
                || value is uint
                || value is long
                || value is ulong
                || value is float
                || value is double
                || value is decimal;
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

    public enum JoinType
    {
        Inner,
        Left
    }

    public class JoinClause
    {
        public JoinType JoinType { get; set; }
        public string ToTableName { get; set; } = string.Empty;
        public string OnClause { get; set; } = string.Empty;
        public (string leftColumn, string rightColumn) ParsedOnClause { get; set; }
    }


    /// <summary>
    /// Represents a parsed SELECT query
    /// </summary>
    public class SelectQuery
    {
        public string TableName { get; set; } = string.Empty;
        public List<string> Columns { get; set; } = new List<string>();
        public List<JoinClause> Joins { get; set; } = new List<JoinClause>();
        public string? WhereClause { get; set; }
        public string? OrderByColumn { get; set; }
        public bool OrderDescending { get; set; }
        public int? Limit { get; set; }
    }
}
