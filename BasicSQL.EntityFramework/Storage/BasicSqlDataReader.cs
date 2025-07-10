using System.Collections;
using System.Data;
using System.Data.Common;

namespace BasicSQL.EntityFramework.Storage
{
    /// <summary>
    /// BasicSQL implementation of DbDataReader.
    /// </summary>
    public class BasicSqlDataReader : DbDataReader
    {
        private readonly List<Dictionary<string, object?>> _rows;
        private readonly List<string> _columns;
        private int _currentRow = -1;

        public BasicSqlDataReader(List<Dictionary<string, object?>> rows, List<string> columns)
        {
            _rows = rows ?? new List<Dictionary<string, object?>>();
            _columns = columns ?? new List<string>();
        }

        public override int Depth => 0;

        public override int FieldCount => _columns.Count;

        public override bool HasRows => _rows.Count > 0;

        private bool _isClosed;
        
        public override bool IsClosed => _isClosed;

        public override int RecordsAffected => _rows.Count;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override bool GetBoolean(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToBoolean(value);
        }

        public override byte GetByte(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToByte(value);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException("GetBytes is not supported in BasicSQL data reader.");
        }

        public override char GetChar(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToChar(value);
        }

        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException("GetChars is not supported in BasicSQL data reader.");
        }

        public override string GetDataTypeName(int ordinal)
        {
            return GetFieldType(ordinal).Name;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToDateTime(value);
        }

        public override decimal GetDecimal(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToDecimal(value);
        }

        public override double GetDouble(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToDouble(value);
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotSupportedException("GetEnumerator is not supported in BasicSQL data reader.");
        }

        public override Type GetFieldType(int ordinal)
        {
            if (_currentRow < 0 || _currentRow >= _rows.Count)
                return typeof(object);

            var value = _rows[_currentRow][_columns[ordinal]];
            return value?.GetType() ?? typeof(object);
        }

        public override float GetFloat(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToSingle(value);
        }

        public override Guid GetGuid(int ordinal)
        {
            var value = GetValue(ordinal);
            return (Guid)value;
        }

        public override short GetInt16(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToInt16(value);
        }

        public override int GetInt32(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToInt32(value);
        }

        public override long GetInt64(int ordinal)
        {
            var value = GetValue(ordinal);
            return Convert.ToInt64(value);
        }

        public override string GetName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= _columns.Count)
                throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");
            
            return _columns[ordinal];
        }

        public override int GetOrdinal(string name)
        {
            var index = _columns.IndexOf(name);
            if (index == -1)
                throw new IndexOutOfRangeException($"Column '{name}' not found.");
            
            return index;
        }

        public override string GetString(int ordinal)
        {
            var value = GetValue(ordinal);
            return value?.ToString() ?? "";
        }

        public override object GetValue(int ordinal)
        {
            if (_currentRow < 0 || _currentRow >= _rows.Count)
                throw new InvalidOperationException("No current row available.");
            
            if (ordinal < 0 || ordinal >= _columns.Count)
                throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range.");

            var columnName = _columns[ordinal];
            var value = _rows[_currentRow].GetValueOrDefault(columnName);
            
            return value ?? DBNull.Value;
        }

        public override int GetValues(object[] values)
        {
            if (_currentRow < 0 || _currentRow >= _rows.Count)
                return 0;

            var count = Math.Min(values.Length, _columns.Count);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            var value = GetValue(ordinal);
            return value == null || value == DBNull.Value;
        }

        public override bool NextResult()
        {
            return false; // BasicSQL doesn't support multiple result sets
        }

        public override bool Read()
        {
            _currentRow++;
            return _currentRow < _rows.Count;
        }

        public override void Close()
        {
            _isClosed = true;
        }
    }
}
