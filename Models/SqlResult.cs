using System;
using System.Collections.Generic;

namespace BasicSQL.Models
{
    /// <summary>
    /// Represents the result of a SQL operation
    /// </summary>
    public class SqlResult
    {
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public string? ErrorMessage { get; set; }
        public List<string> Columns { get; set; } = new List<string>();
        public List<Dictionary<string, object?>> Rows { get; set; } = new List<Dictionary<string, object?>>();
        public int RowsAffected { get; set; }
        public List<string> Tables { get; set; } = new List<string>();

        public static SqlResult CreateSuccess(string message, int rowsAffected = 0)
        {
            return new SqlResult
            {
                Success = true,
                Message = message,
                RowsAffected = rowsAffected
            };
        }

        public static SqlResult CreateError(string errorMessage)
        {
            return new SqlResult
            {
                Success = false,
                ErrorMessage = errorMessage
            };
        }

        public static SqlResult CreateQueryResult(List<string> columns, List<Dictionary<string, object?>> rows)
        {
            return new SqlResult
            {
                Success = true,
                Columns = columns,
                Rows = rows,
                RowsAffected = rows.Count,
                Message = $"{rows.Count} row(s) returned"
            };
        }

        public static SqlResult CreateTableListResult(List<string> tables)
        {
            return new SqlResult
            {
                Success = true,
                Tables = tables,
                Message = $"{tables.Count} table(s) found"
            };
        }

        public bool IsQueryResult => Columns.Count > 0 || Rows.Count > 0;
        public bool IsTableListResult => Tables.Count > 0;

        public override string ToString()
        {
            if (!Success)
                return $"ERROR: {ErrorMessage}";
            
            if (IsQueryResult)
                return $"Query returned {Rows.Count} row(s)";
            
            if (IsTableListResult)
                return $"Found {Tables.Count} table(s)";
            
            return Message;
        }
    }
}
