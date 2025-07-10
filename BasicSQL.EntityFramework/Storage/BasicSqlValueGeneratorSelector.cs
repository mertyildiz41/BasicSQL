using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.EntityFrameworkCore.ValueGeneration.Internal;
using System;
using System.Linq;
using System.Threading;

namespace BasicSQL.EntityFramework.Storage
{
    public class BasicSqlValueGeneratorSelector : ValueGeneratorSelector
    {
        public BasicSqlValueGeneratorSelector(ValueGeneratorSelectorDependencies dependencies)
            : base(dependencies)
        {
        }

        public override ValueGenerator Create(IProperty property, ITypeBase typeBase)
        {
            var type = property.ClrType;
            
            // Remove nullable wrapper
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                type = type.GetGenericArguments()[0];
            }

            if (property.ValueGenerated == ValueGenerated.OnAdd)
            {
                // For auto-increment int/long columns, use EF Core's built-in temporary value generators
                if (type == typeof(int))
                {
                    return new TemporaryIntValueGenerator();
                }
                
                if (type == typeof(long))
                {
                    return new TemporaryLongValueGenerator();
                }

                if (type == typeof(Guid))
                {
                    return new GuidValueGenerator();
                }
            }

            // For other cases, try base implementation first
            try
            {
                return base.Create(property, typeBase);
            }
            catch (NotSupportedException)
            {
                // If base implementation fails, throw a more helpful error
                throw new NotSupportedException(
                    $"The property '{property.DeclaringType.DisplayName()}.{property.Name}' of type '{type.Name}' " +
                    $"does not have a supported value generator in BasicSQL provider. " +
                    $"Consider configuring a value generator manually or using a supported type.");
            }
        }
    }
}
