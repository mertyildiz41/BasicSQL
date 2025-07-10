using System.Data;
using System.Data.Common;

namespace BasicSQL.EntityFramework.Storage
{
    /// <summary>
    /// BasicSQL implementation of DbConnection.
    /// </summary>
    public class BasicSqlDbConnection : DbConnection
    {
        private readonly string _connectionString;
        private ConnectionState _state = ConnectionState.Closed;

        public BasicSqlDbConnection(string? connectionString)
        {
            _connectionString = connectionString ?? "";
        }

        /// <summary>
        /// Gets or sets the last inserted ID from an INSERT operation.
        /// This is used to return generated key values to EF Core.
        /// </summary>
        public int? LastInsertedId { get; set; }

        public override string ConnectionString
        {
            get => _connectionString;
            set => throw new NotSupportedException("Cannot change connection string after initialization.");
        }

        public override string Database => "BasicSQL";

        public override string DataSource => _connectionString;

        public override string ServerVersion => "1.0.0";

        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("BasicSQL does not support changing databases.");
        }

        public override void Close()
        {
            _state = ConnectionState.Closed;
        }

        public override void Open()
        {
            _state = ConnectionState.Open;
        }

        public override Task OpenAsync(CancellationToken cancellationToken)
        {
            _state = ConnectionState.Open;
            return Task.CompletedTask;
        }

        public override Task CloseAsync()
        {
            _state = ConnectionState.Closed;
            return Task.CompletedTask;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("BasicSQL does not support transactions.");
        }

        protected override DbCommand CreateDbCommand()
        {
            return new BasicSqlDbCommand(this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
            base.Dispose(disposing);
        }
    }
}
