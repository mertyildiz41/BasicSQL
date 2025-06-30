using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using BasicSQL.EntityFramework.Infrastructure;

namespace BasicSQL.EntityFramework;

/// <summary>
/// Extension methods for configuring BasicSQL with Entity Framework Core
/// </summary>
public static class BasicSqlDbContextExtensions
{
    /// <summary>
    /// Configures the context to use BasicSQL as the database provider
    /// </summary>
    public static DbContextOptionsBuilder UseBasicSql(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString)
    {
        if (optionsBuilder == null)
            throw new ArgumentNullException(nameof(optionsBuilder));

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionString));

        var extension = optionsBuilder.Options.FindExtension<BasicSqlOptionsExtension>() ?? new BasicSqlOptionsExtension();
        extension = (extension.ConnectionString == connectionString) 
            ? extension 
            : (BasicSqlOptionsExtension)extension.WithConnectionString(connectionString);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        return optionsBuilder;
    }

    /// <summary>
    /// Configures the context to use BasicSQL as the database provider
    /// </summary>
    public static DbContextOptionsBuilder<TContext> UseBasicSql<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseBasicSql(
            (DbContextOptionsBuilder)optionsBuilder, connectionString);
}
