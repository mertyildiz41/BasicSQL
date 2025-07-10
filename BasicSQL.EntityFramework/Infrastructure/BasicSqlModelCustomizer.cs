using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace BasicSQL.EntityFramework.Infrastructure
{
    /// <summary>
    /// BasicSQL model customizer to configure auto-increment properties correctly.
    /// </summary>
    public class BasicSqlModelCustomizer : RelationalModelCustomizer
    {
        public BasicSqlModelCustomizer(ModelCustomizerDependencies dependencies)
            : base(dependencies)
        {
        }

        public override void Customize(ModelBuilder modelBuilder, DbContext context)
        {
            base.Customize(modelBuilder, context);

            // Configure auto-increment properties for BasicSQL
            foreach (var entityType in modelBuilder.Model.GetEntityTypes())
            {
                foreach (var property in entityType.GetProperties())
                {
                    // For auto-increment integer primary keys
                    if (property.IsPrimaryKey() && 
                        property.ValueGenerated == Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAdd &&
                        (property.ClrType == typeof(int) || property.ClrType == typeof(long)))
                    {
                        // Ensure EF Core knows this is an auto-increment column
                        property.SetValueGenerationStrategy(BasicSqlValueGenerationStrategy.AutoIncrement);
                        property.SetDefaultValueSql(null);
                        
                        // Configure the column type
                        property.SetColumnType(property.ClrType == typeof(long) ? "BIGINT" : "INTEGER");
                        
                        Console.WriteLine($"Configured auto-increment for {entityType.ClrType.Name}.{property.Name}");
                    }
                }
            }
        }
    }

    /// <summary>
    /// BasicSQL value generation strategy enum.
    /// </summary>
    public enum BasicSqlValueGenerationStrategy
    {
        None,
        AutoIncrement
    }
}
