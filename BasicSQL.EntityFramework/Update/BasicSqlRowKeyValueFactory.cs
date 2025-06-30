using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Linq;

namespace BasicSQL.EntityFramework.Update
{
    /// <summary>
    /// Custom row key value factory for BasicSQL that handles auto-increment columns.
    /// </summary>
    public class BasicSqlRowKeyValueFactory : SimpleRowKeyValueFactory<object>
    {
        public BasicSqlRowKeyValueFactory(IKey key) : base(key)
        {
        }

        public override object CreateKeyValue(IReadOnlyModificationCommand command, bool fromOriginalValues)
        {
            // For auto-increment columns, we need to handle the case where the key value 
            // is available from the temporary value generator but might not be in the 
            // standard column modification lookup
            
            var keyProperties = Key.Properties;
            
            // If it's a single-column key
            if (keyProperties.Count == 1)
            {
                var keyProperty = keyProperties[0];
                
                // Try to find the column modification for this property
                var columnModification = command.ColumnModifications
                    .FirstOrDefault(c => c.Property?.Name == keyProperty.Name);
                
                if (columnModification != null)
                {
                    // Get the value from the column modification
                    var value = fromOriginalValues ? columnModification.OriginalValue : columnModification.Value;
                    
                    // If the value is not null, use it
                    if (value != null)
                    {
                        return value;
                    }
                }
                
                // For auto-increment columns with temporary values, we might need to 
                // extract the value from the entity entry
                if (keyProperty.ValueGenerated == ValueGenerated.OnAdd)
                {
                    // Use a placeholder value for auto-increment columns
                    // This will be replaced after the INSERT operation
                    return CreateTempKeyValue(keyProperty);
                }
            }
            
            // Fall back to base implementation for other cases
            try
            {
                return base.CreateKeyValue(command, fromOriginalValues);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("key column") && ex.Message.Contains("is null"))
            {
                // If the base implementation fails due to null key, create a temporary value
                return CreateTempKeyValue(keyProperties[0]);
            }
        }
        
        private object CreateTempKeyValue(IProperty keyProperty)
        {
            var type = keyProperty.ClrType;
            
            // Remove nullable wrapper
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                type = type.GetGenericArguments()[0];
            }
            
            if (type == typeof(int))
            {
                return -999999; // Temporary negative value
            }
            
            if (type == typeof(long))
            {
                return -999999L; // Temporary negative value
            }
            
            if (type == typeof(Guid))
            {
                return Guid.NewGuid();
            }
            
            // For other types, return the default value
            return Activator.CreateInstance(type) ?? throw new InvalidOperationException($"Cannot create temporary value for type {type}");
        }
    }
}
