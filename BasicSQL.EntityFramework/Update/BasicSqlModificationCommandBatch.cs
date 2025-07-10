using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using BasicSQL.EntityFramework.Storage;
using Microsoft.EntityFrameworkCore.Metadata;

namespace BasicSQL.EntityFramework.Update
{
    public class BasicSqlModificationCommandBatch : SingularModificationCommandBatch
    {
        public BasicSqlModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
            : base(dependencies)
        {
        }

        public override void Execute(IRelationalConnection connection)
        {
            base.Execute(connection);
            
            // After executing the command, check if we have any auto-generated values to propagate
            if (connection.DbConnection is BasicSqlDbConnection basicConnection && 
                basicConnection.LastInsertedId.HasValue)
            {
                PropagateGeneratedValues(basicConnection.LastInsertedId.Value);
                basicConnection.LastInsertedId = null; // Reset for next operation
            }
        }

        public override async Task ExecuteAsync(IRelationalConnection connection, CancellationToken cancellationToken = default)
        {
            await base.ExecuteAsync(connection, cancellationToken);
            
            // After executing the command, check if we have any auto-generated values to propagate
            if (connection.DbConnection is BasicSqlDbConnection basicConnection && 
                basicConnection.LastInsertedId.HasValue)
            {
                PropagateGeneratedValues(basicConnection.LastInsertedId.Value);
                basicConnection.LastInsertedId = null; // Reset for next operation
            }
        }

        private void PropagateGeneratedValues(int generatedId)
        {
            // Find the INSERT command that needs the generated ID
            foreach (var command in ModificationCommands)
            {
                foreach (var columnModification in command.ColumnModifications)
                {
                    if (columnModification.Property?.ValueGenerated == ValueGenerated.OnAdd &&
                        columnModification.Property.IsPrimaryKey() &&
                        (columnModification.Property.ClrType == typeof(int) || columnModification.Property.ClrType == typeof(long)))
                    {
                        // Set the generated value
                        columnModification.Value = generatedId;
                        break;
                    }
                }
            }
        }
    }
}
