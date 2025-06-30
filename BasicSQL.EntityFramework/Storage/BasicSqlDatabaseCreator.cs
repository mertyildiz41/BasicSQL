using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore; // Add the relational extensions namespace
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BasicSQL.EntityFramework.Storage
{
    public class BasicSqlDatabaseCreator : IRelationalDatabaseCreator
    {
        private readonly IRelationalConnection _connection;
        private readonly ICurrentDbContext _currentDbContext;

        public BasicSqlDatabaseCreator(IRelationalConnection connection, ICurrentDbContext currentDbContext)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _currentDbContext = currentDbContext ?? throw new ArgumentNullException(nameof(currentDbContext));
        }

        public bool CanConnect()
        {
            try
            {
                _connection.Open();
                _connection.Close();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public async Task<bool> CanConnectAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await _connection.OpenAsync(cancellationToken);
                await _connection.CloseAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Create()
        {
            // For BasicSQL, we might not need to create a physical database file
            // The database is created when we first connect or execute commands
        }

        public async Task CreateAsync(CancellationToken cancellationToken = default)
        {
            // For BasicSQL, we might not need to create a physical database file
            // The database is created when we first connect or execute commands
            await Task.CompletedTask;
        }

        public void CreateTables()
        {
            var model = _currentDbContext.Context.Model;
            foreach (var entityType in model.GetEntityTypes())
            {
                var createTableSql = GenerateCreateTableSql(entityType);
                if (!string.IsNullOrEmpty(createTableSql))
                {
                    var command = _connection.DbConnection.CreateCommand();
                    command.CommandText = createTableSql;
                    _connection.Open();
                    try
                    {
                        command.ExecuteNonQuery();
                    }
                    finally
                    {
                        _connection.Close();
                    }
                }
            }
        }

        public async Task CreateTablesAsync(CancellationToken cancellationToken = default)
        {
            var model = _currentDbContext.Context.Model;
            foreach (var entityType in model.GetEntityTypes())
            {
                var createTableSql = GenerateCreateTableSql(entityType);
                if (!string.IsNullOrEmpty(createTableSql))
                {
                    var command = _connection.DbConnection.CreateCommand();
                    command.CommandText = createTableSql;
                    await _connection.OpenAsync(cancellationToken);
                    try
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken);
                    }
                    finally
                    {
                        await _connection.CloseAsync();
                    }
                }
            }
        }

        public void Delete()
        {
            // For BasicSQL, this could delete the database files
            // Implementation depends on how BasicSQL stores data
        }

        public async Task DeleteAsync(CancellationToken cancellationToken = default)
        {
            // For BasicSQL, this could delete the database files
            // Implementation depends on how BasicSQL stores data
            await Task.CompletedTask;
        }

        public bool Exists()
        {
            // For BasicSQL, we can assume the database always exists since it's file-based
            // and tables are created on demand
            return true;
        }

        public async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            // For BasicSQL, we can assume the database always exists since it's file-based
            // and tables are created on demand
            await Task.CompletedTask;
            return true;
        }

        public bool HasTables()
        {
            // For BasicSQL, we could check if any table files exist
            // For now, return true as tables are created on demand
            return true;
        }

        public async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
        {
            // For BasicSQL, we could check if any table files exist
            // For now, return true as tables are created on demand
            await Task.CompletedTask;
            return true;
        }

        public bool EnsureCreated()
        {
            // For BasicSQL, since tables are created on demand and the database
            // doesn't require explicit creation, we'll return true to indicate
            // the database structure is ready
            CreateTables();
            return true;
        }

        public async Task<bool> EnsureCreatedAsync(CancellationToken cancellationToken = default)
        {
            // For BasicSQL, since tables are created on demand and the database
            // doesn't require explicit creation, we'll return true to indicate
            // the database structure is ready
            await CreateTablesAsync(cancellationToken);
            return true;
        }

        public bool EnsureDeleted()
        {
            if (Exists())
            {
                Delete();
                return true;
            }
            return false;
        }

        public async Task<bool> EnsureDeletedAsync(CancellationToken cancellationToken = default)
        {
            if (await ExistsAsync(cancellationToken))
            {
                await DeleteAsync(cancellationToken);
                return true;
            }
            return false;
        }

        public string GenerateCreateScript()
        {
            var model = _currentDbContext.Context.Model;
            var script = new StringBuilder();
            
            foreach (var entityType in model.GetEntityTypes())
            {
                var createTableSql = GenerateCreateTableSql(entityType);
                if (!string.IsNullOrEmpty(createTableSql))
                {
                    script.AppendLine(createTableSql);
                    script.AppendLine();
                }
            }
            
            return script.ToString();
        }

        private string GenerateCreateTableSql(IEntityType entityType)
        {
            var tableName = entityType.GetTableName() ?? entityType.Name;
            if (string.IsNullOrEmpty(tableName))
                return string.Empty;

            var sql = new StringBuilder();
            sql.Append($"CREATE TABLE {tableName} (");

            var columns = entityType.GetProperties().ToList();
            for (int i = 0; i < columns.Count; i++)
            {
                var property = columns[i];
                var columnName = property.GetColumnName() ?? property.Name;
                var columnType = GetColumnType(property);
                
                if (i > 0)
                    sql.Append(", ");
                
                sql.Append($"{columnName} {columnType}");
                
                // Check if this is an auto-increment/identity column
                if (property.ValueGenerated == Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAdd)
                {
                    if (property.ClrType == typeof(int) || property.ClrType == typeof(long))
                    {
                        sql.Append(" AUTO_INCREMENT");
                    }
                }
                
                // Check if this is a primary key
                if (property.IsPrimaryKey())
                {
                    sql.Append(" PRIMARY KEY");
                }
                
                // Check if not nullable
                if (!property.IsNullable)
                {
                    sql.Append(" NOT NULL");
                }
            }

            sql.Append(")");
            return sql.ToString();
        }

        private string GetColumnType(IProperty property)
        {
            // Map .NET types to BasicSQL types
            return property.ClrType.Name switch
            {
                nameof(Int32) => "INTEGER",
                nameof(Int64) => "INTEGER",
                nameof(String) => "TEXT",
                nameof(Boolean) => "INTEGER",
                nameof(DateTime) => "TEXT",
                nameof(Double) => "REAL",
                nameof(Single) => "REAL",
                nameof(Decimal) => "REAL",
                _ => "TEXT"
            };
        }
    }
}
