using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using BasicSQL.EntityFramework.Infrastructure;
using System.Data.Common;

namespace BasicSQL.EntityFramework.Storage
{
    /// <summary>
    /// BasicSQL implementation of IRelationalConnection.
    /// </summary>
    public class BasicSqlRelationalConnection : IRelationalConnection
    {
        private readonly RelationalConnectionDependencies _dependencies;
        private BasicSqlDbConnection? _connection;

        public string? ConnectionString 
        { 
            get => _dependencies.ContextOptions.FindExtension<BasicSqlOptionsExtension>()?.ConnectionString;
            set => throw new NotSupportedException("Cannot change connection string after initialization.");
        }
        
        public DbConnection DbConnection 
        { 
            get => _connection ??= new BasicSqlDbConnection(ConnectionString);
            set => throw new NotSupportedException("Cannot change DbConnection on BasicSqlRelationalConnection.");
        }
        
        public Guid ConnectionId { get; } = Guid.NewGuid();
        
        public int? CommandTimeout { get; set; }
        
        public bool IsMultipleActiveResultSetsEnabled => false;
        
        public IDbContextTransaction? CurrentTransaction { get; private set; }

        public DbContext? Context => null;

        public BasicSqlRelationalConnection(RelationalConnectionDependencies dependencies)
        {
            _dependencies = dependencies;
        }

        public IDbContextTransaction BeginTransaction()
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public IDbContextTransaction BeginTransaction(System.Data.IsolationLevel isolationLevel)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public Task<IDbContextTransaction> BeginTransactionAsync(System.Data.IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public void CommitTransaction()
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public Task CommitTransactionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public void RollbackTransaction()
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public Task RollbackTransactionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public IDbContextTransaction? UseTransaction(DbTransaction? transaction)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public IDbContextTransaction? UseTransaction(DbTransaction? transaction, Guid transactionId)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public Task<IDbContextTransaction?> UseTransactionAsync(DbTransaction? transaction, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public Task<IDbContextTransaction?> UseTransactionAsync(DbTransaction? transaction, Guid transactionId, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException("Transactions are not yet supported by BasicSQL Entity Framework provider.");
        }

        public bool Open(bool errorsExpected = false)
        {
            if (DbConnection.State != System.Data.ConnectionState.Open)
            {
                DbConnection.Open();
                return true;
            }
            return false;
        }

        public async Task<bool> OpenAsync(CancellationToken cancellationToken, bool errorsExpected = false)
        {
            if (DbConnection.State != System.Data.ConnectionState.Open)
            {
                await DbConnection.OpenAsync(cancellationToken);
                return true;
            }
            return false;
        }

        public bool Close()
        {
            if (DbConnection.State != System.Data.ConnectionState.Closed)
            {
                DbConnection.Close();
                return true;
            }
            return false;
        }

        public async Task<bool> CloseAsync()
        {
            if (DbConnection.State != System.Data.ConnectionState.Closed)
            {
                await DbConnection.CloseAsync();
                return true;
            }
            return false;
        }

        public void SetDbConnection(DbConnection? connection, bool contextOwnsConnection = true)
        {
            throw new NotSupportedException("SetDbConnection is not supported by BasicSqlRelationalConnection.");
        }

        public IRelationalCommand RentCommand()
        {
            throw new NotImplementedException("RentCommand is not yet implemented for BasicSQL Entity Framework provider.");
        }

        public void ReturnCommand(IRelationalCommand command)
        {
            // No-op for now
        }

        public void ResetState()
        {
            // No-op for now
        }

        public Task ResetStateAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            return _connection?.DisposeAsync() ?? ValueTask.CompletedTask;
        }
    }
}
