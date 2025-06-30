using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BasicSQL.EntityFramework.Storage
{
    public class BasicSqlSocketRelationalConnection : RelationalConnection
    {
        public BasicSqlSocketRelationalConnection(RelationalConnectionDependencies dependencies)
            : base(dependencies) { }

        protected override DbConnection CreateDbConnection()
        {
            return new BasicSqlSocketDbConnection(ConnectionString);
        }
    }

    public class BasicSqlSocketDbConnection : DbConnection
    {
        private string _connectionString;
        public BasicSqlSocketDbConnection(string connectionString) => _connectionString = connectionString;
        public override string ConnectionString { get => _connectionString; set => _connectionString = value; }
        public override string Database => "BasicSQL";
        public override string DataSource => "localhost";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        protected override DbCommand CreateDbCommand() => new BasicSqlSocketDbCommand(this);
    }

    public class BasicSqlSocketDbCommand : DbCommand
    {
        private readonly BasicSqlSocketDbConnection _connection;
        public BasicSqlSocketDbCommand(BasicSqlSocketDbConnection connection) => _connection = connection;
        public override string CommandText { get; set; }
        public override int ExecuteNonQuery()
        {
            return SendSql(CommandText);
        }
        public override object ExecuteScalar()
        {
            return SendSql(CommandText);
        }
        public override void Cancel() { }
        public override int CommandTimeout { get; set; } = 30;
        public override CommandType CommandType { get; set; } = CommandType.Text;
        public override UpdateRowSource UpdatedRowSource { get; set; } = UpdateRowSource.None;
        protected override DbConnection DbConnection { get => _connection; set { } }
        protected override DbParameterCollection DbParameterCollection => new BasicSqlSocketDbParameterCollection();
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        protected override DbParameter CreateDbParameter() => new BasicSqlSocketDbParameter();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            // For simplicity, just return a dummy reader
            return new BasicSqlSocketDbDataReader(SendSqlRaw(CommandText));
        }
        private int SendSql(string sql)
        {
            var response = SendSqlRaw(sql);
            // Parse response for affected rows if possible
            return 1;
        }
        private string SendSqlRaw(string sql)
        {
            // Parse connection string for host/port
            var host = "localhost";
            var port = 4162;
            var parts = _connection.ConnectionString.Split(';');
            foreach (var part in parts)
            {
                var kv = part.Split('=');
                if (kv.Length == 2)
                {
                    if (kv[0].Trim().ToLower() == "host") host = kv[1].Trim();
                    if (kv[0].Trim().ToLower() == "port" && int.TryParse(kv[1], out int p)) port = p;
                }
            }
            using (var client = new TcpClient(host, port))
            using (var stream = client.GetStream())
            using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            {
                writer.WriteLine(sql);
                return reader.ReadToEnd();
            }
        }
    }

    public class BasicSqlSocketDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; }
        public override string SourceColumn { get; set; }
        public override object Value { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override int Size { get; set; }
        public override void ResetDbType() { }
    }
    public class BasicSqlSocketDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _parameters = new();
        public override int Count => _parameters.Count;
        public override object SyncRoot => this;
        public override int Add(object value) { _parameters.Add((DbParameter)value); return _parameters.Count - 1; }
        public override void AddRange(Array values) { foreach (var v in values) Add(v); }
        public override void Clear() => _parameters.Clear();
        public override bool Contains(object value) => _parameters.Contains((DbParameter)value);
        public override bool Contains(string value) => _parameters.Any(p => p.ParameterName == value);
        public override void CopyTo(Array array, int index) => _parameters.ToArray().CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _parameters.GetEnumerator();
        public override int IndexOf(object value) => _parameters.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName) => _parameters.FindIndex(p => p.ParameterName == parameterName);
        public override void Insert(int index, object value) => _parameters.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _parameters.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _parameters.RemoveAt(index);
        public override void RemoveAt(string parameterName) => _parameters.RemoveAt(IndexOf(parameterName));
        protected override DbParameter GetParameter(int index) => (DbParameter)_parameters[index];
        protected override DbParameter GetParameter(string parameterName) => _parameters.First(p => p.ParameterName == parameterName);
        protected override void SetParameter(int index, DbParameter value) => _parameters[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value) => _parameters[IndexOf(parameterName)] = value;
    }
    public class BasicSqlSocketDbDataReader : DbDataReader
    {
        private readonly string _data;
        private bool _read = false;
        public BasicSqlSocketDbDataReader(string data) { _data = data; }
        public override bool Read() { if (!_read) { _read = true; return true; } return false; }
        public override int FieldCount => 1;
        public override object GetValue(int ordinal) => _data;
        public override bool HasRows => !string.IsNullOrEmpty(_data);
        public override bool IsClosed => false;
        public override int RecordsAffected => 1;
        public override bool NextResult() => false;
        public override int Depth => 0;
        public override string GetName(int ordinal) => "Result";
        public override string GetDataTypeName(int ordinal) => "string";
        public override Type GetFieldType(int ordinal) => typeof(string);
        public override IEnumerator GetEnumerator() { yield return _data; }
        public override bool GetBoolean(int ordinal) => false;
        public override byte GetByte(int ordinal) => 0;
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => 0;
        public override char GetChar(int ordinal) => ' ';
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => 0;
        public override Guid GetGuid(int ordinal) => Guid.Empty;
        public override short GetInt16(int ordinal) => 0;
        public override int GetInt32(int ordinal) => 0;
        public override long GetInt64(int ordinal) => 0;
        public override float GetFloat(int ordinal) => 0;
        public override double GetDouble(int ordinal) => 0;
        public override string GetString(int ordinal) => _data;
        public override decimal GetDecimal(int ordinal) => 0;
        public override DateTime GetDateTime(int ordinal) => DateTime.MinValue;
        public override int GetOrdinal(string name) => 0;
        public override bool IsDBNull(int ordinal) => false;
        public override object this[int ordinal] => _data;
        public override object this[string name] => _data;
    }
}
