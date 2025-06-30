using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using System;
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
                // For auto-increment int/long columns, provide temporary value generators
                // These will be replaced by the actual database-generated values
                if (type == typeof(int))
                {
                    return new BasicSqlIntValueGenerator();
                }
                
                if (type == typeof(long))
                {
                    return new BasicSqlLongValueGenerator();
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

    public class BasicSqlIntValueGenerator : ValueGenerator<int>
    {
        private static int _current = 0;
        
        public override bool GeneratesTemporaryValues => false; // These are permanent values
        
        public override int Next(EntityEntry entry)
        {
            // Generate a positive permanent value for EF Core to use
            // This value will be used in the modification command for row key creation
            return Interlocked.Increment(ref _current);
        }
    }

    public class BasicSqlLongValueGenerator : ValueGenerator<long>
    {
        private static long _current = 0L;
        
        public override bool GeneratesTemporaryValues => false; // These are permanent values
        
        public override long Next(EntityEntry entry)
        {
            // Generate a positive permanent value for EF Core to use
            // This value will be used in the modification command for row key creation
            return Interlocked.Increment(ref _current);
        }
    }
}
