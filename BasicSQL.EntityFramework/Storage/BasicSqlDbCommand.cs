using System.Collections;
using System.Data;
using System.Data.Common;
using BasicSQL.Core;

namespace BasicSQL.EntityFramework.Storage
{
    /// <summary>
    /// BasicSQL implementation of DbCommand.
    /// </summary>
    public class BasicSqlDbCommand : DbCommand
    {
        private readonly BasicSqlDbConnection _connection;
        private string _commandText = "";

        public BasicSqlDbCommand(BasicSqlDbConnection connection)
        {
            _connection = connection;
        }

        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? "";
        }

        public override int CommandTimeout { get; set; } = 30;

        public override CommandType CommandType { get; set; } = CommandType.Text;

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => throw new NotSupportedException("Cannot change connection on BasicSqlDbCommand.");
        }

        protected override DbParameterCollection DbParameterCollection { get; } = new BasicSqlParameterCollection();

        protected override DbTransaction? DbTransaction { get; set; }

        public override void Cancel()
        {
            // BasicSQL doesn't support cancellation
        }

        public override int ExecuteNonQuery()
        {
            if (string.IsNullOrEmpty(CommandText))
                return 0;

            try
            {
                // Extract the database path from the connection string
                var connectionString = _connection.ConnectionString ?? "";
                Console.WriteLine($"DbCommand: Connection string = '{connectionString}'");
                
                if (string.IsNullOrEmpty(connectionString))
                {
                    throw new InvalidOperationException("Connection string is null or empty");
                }
                
                var dataSource = ExtractDataSource(connectionString);
                Console.WriteLine($"DbCommand: Extracted data source = '{dataSource}'");
                
                if (string.IsNullOrEmpty(dataSource))
                {
                    throw new InvalidOperationException($"Could not determine database path from connection string: '{connectionString}'");
                }

                // Create BasicSQL engine instance
                var engine = new BinarySqlEngine(dataSource);
                Console.WriteLine($"DbCommand: Created engine for path '{dataSource}', executing: {CommandText}");
                
                // Execute the SQL command
                var result = engine.Execute(CommandText);
                
                if (!result.Success)
                {
                    throw new InvalidOperationException($"SQL execution failed: {result.Message}");
                }
                
                Console.WriteLine($"DbCommand: Execution result - Success: {result.Success}, Message: {result.Message}, RowsAffected: {result.RowsAffected}");
                
                // Store generated key for INSERT operations that return a row ID
                if (CommandText.Trim().StartsWith("INSERT", StringComparison.OrdinalIgnoreCase) && 
                    result.Message.Contains("Row ID:"))
                {
                    // Extract the Row ID from the message like "1 row inserted with binary storage (Row ID: 123)"
                    var match = System.Text.RegularExpressions.Regex.Match(result.Message, @"Row ID:\s*(\d+)");
                    if (match.Success && int.TryParse(match.Groups[1].Value, out var rowId))
                    {
                        // Store the generated key in the connection for EF Core to retrieve
                        _connection.LastInsertedId = rowId;
                        Console.WriteLine($"DbCommand: Stored last inserted ID = {rowId}");
                    }
                }
                
                // Return the number of affected rows
                return result.RowsAffected;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DbCommand: Exception occurred - {ex.Message}");
                throw new InvalidOperationException($"Failed to execute command: {ex.Message}", ex);
            }
        }

        private string ExtractDataSource(string connectionString)
        {
            // Parse connection string to extract Data Source
            var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                var keyValue = part.Split('=', 2);
                if (keyValue.Length == 2 && keyValue[0].Trim().Equals("Data Source", StringComparison.OrdinalIgnoreCase))
                {
                    return keyValue[1].Trim();
                }
            }
            
            // If no explicit Data Source, check if the whole connection string is just a path
            if (!connectionString.Contains('=') && !connectionString.Contains(';'))
            {
                return connectionString.Trim();
            }
            
            return "";
        }

        public override object? ExecuteScalar()
        {
            if (string.IsNullOrEmpty(CommandText))
                return null;

            try
            {
                // Extract the database path from the connection string
                var connectionString = _connection.ConnectionString ?? "";
                if (string.IsNullOrEmpty(connectionString))
                {
                    throw new InvalidOperationException("Connection string is null or empty");
                }
                
                var dataSource = ExtractDataSource(connectionString);
                if (string.IsNullOrEmpty(dataSource))
                {
                    throw new InvalidOperationException($"Could not determine database path from connection string: '{connectionString}'");
                }

                // Create BasicSQL engine instance
                var engine = new BinarySqlEngine(dataSource);
                
                // Execute the SQL command
                var result = engine.Execute(CommandText);
                
                if (!result.Success)
                {
                    throw new InvalidOperationException($"SQL execution failed: {result.Message}");
                }
                
                // For INSERT operations, return the generated ID
                if (CommandText.Trim().StartsWith("INSERT", StringComparison.OrdinalIgnoreCase) && 
                    result.Message.Contains("Row ID:"))
                {
                    // Extract the Row ID from the message like "1 row inserted with binary storage (Row ID: 123)"
                    var match = System.Text.RegularExpressions.Regex.Match(result.Message, @"Row ID:\s*(\d+)");
                    if (match.Success && int.TryParse(match.Groups[1].Value, out var rowId))
                    {
                        return rowId;
                    }
                }
                
                // For SELECT operations, return the first column of the first row
                if (result.Rows?.Count > 0)
                {
                    var firstRow = result.Rows[0];
                    return firstRow.Values.FirstOrDefault();
                }
                
                return null;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to execute scalar command: {ex.Message}", ex);
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (string.IsNullOrEmpty(CommandText))
                return new BasicSqlDataReader(new List<Dictionary<string, object?>>(), new List<string>());

            try
            {
                // Extract the database path from the connection string
                var connectionString = _connection.ConnectionString ?? "";
                if (string.IsNullOrEmpty(connectionString))
                {
                    throw new InvalidOperationException("Connection string is null or empty");
                }
                
                var dataSource = ExtractDataSource(connectionString);
                if (string.IsNullOrEmpty(dataSource))
                {
                    throw new InvalidOperationException($"Could not determine database path from connection string: '{connectionString}'");
                }

                // Create BasicSQL engine instance
                var engine = new BinarySqlEngine(dataSource);
                
                // Execute the SQL command
                var result = engine.Execute(CommandText);
                
                if (!result.Success)
                {
                    throw new InvalidOperationException($"SQL execution failed: {result.Message}");
                }
                
                // For INSERT operations that need to return generated values
                if (CommandText.Trim().StartsWith("INSERT", StringComparison.OrdinalIgnoreCase) && 
                    result.Message.Contains("Row ID:"))
                {
                    // Extract the Row ID from the message like "1 row inserted with binary storage (Row ID: 123)"
                    var match = System.Text.RegularExpressions.Regex.Match(result.Message, @"Row ID:\s*(\d+)");
                    if (match.Success && int.TryParse(match.Groups[1].Value, out var rowId))
                    {
                        // Return a synthetic result set with the generated ID
                        var generatedRow = new Dictionary<string, object?> { { "generated_id", rowId } };
                        return new BasicSqlDataReader(new List<Dictionary<string, object?>> { generatedRow }, new List<string> { "generated_id" });
                    }
                }
                
                // For regular SELECT operations
                var rows = result.Rows ?? new List<Dictionary<string, object?>>();
                var columns = rows.Count > 0 ? rows[0].Keys.ToList() : new List<string>();
                
                return new BasicSqlDataReader(rows, columns);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to execute reader command: {ex.Message}", ex);
            }
        }

        public override void Prepare()
        {
            // BasicSQL doesn't support prepared statements
        }

        protected override DbParameter CreateDbParameter()
        {
            return new BasicSqlParameter();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// BasicSQL implementation of DbParameterCollection.
    /// </summary>
    public class BasicSqlParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _parameters = new();

        public override int Count => _parameters.Count;

        public override object SyncRoot => _parameters;

        public override int Add(object value)
        {
            if (value is DbParameter parameter)
            {
                _parameters.Add(parameter);
                return _parameters.Count - 1;
            }
            throw new ArgumentException("Value must be a DbParameter");
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value);
            }
        }

        public override void Clear()
        {
            _parameters.Clear();
        }

        public override bool Contains(object value)
        {
            return _parameters.Contains(value);
        }

        public override bool Contains(string value)
        {
            return _parameters.Any(p => string.Equals(p.ParameterName, value, StringComparison.OrdinalIgnoreCase));
        }

        public override void CopyTo(Array array, int index)
        {
            ((ICollection)_parameters).CopyTo(array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return ((IEnumerable)_parameters).GetEnumerator();
        }

        public override int IndexOf(object value)
        {
            return _parameters.IndexOf((DbParameter)value);
        }

        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < _parameters.Count; i++)
            {
                if (string.Equals(_parameters[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                    return i;
            }
            return -1;
        }

        public override void Insert(int index, object value)
        {
            _parameters.Insert(index, (DbParameter)value);
        }

        public override void Remove(object value)
        {
            _parameters.Remove((DbParameter)value);
        }

        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
                RemoveAt(index);
        }

        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            var index = IndexOf(parameterName);
            return index >= 0 ? _parameters[index] : throw new ArgumentException($"Parameter '{parameterName}' not found");
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            _parameters[index] = value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
                _parameters[index] = value;
            else
                throw new ArgumentException($"Parameter '{parameterName}' not found");
        }
    }

    /// <summary>
    /// BasicSQL implementation of DbParameter.
    /// </summary>
    public class BasicSqlParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; } = "";
        public override int Size { get; set; }
        public override string SourceColumn { get; set; } = "";
        public override bool SourceColumnNullMapping { get; set; }
        public override object? Value { get; set; }

        public override void ResetDbType()
        {
            DbType = DbType.Object;
        }
    }
}
