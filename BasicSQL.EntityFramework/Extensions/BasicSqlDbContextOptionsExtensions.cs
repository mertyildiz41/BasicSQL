using BasicSQL.EntityFramework.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace BasicSQL.EntityFramework.Extensions
{
    /// <summary>
    /// BasicSQL-specific extension methods for <see cref="DbContextOptionsBuilder"/>.
    /// </summary>
    public static class BasicSqlDbContextOptionsExtensions
    {
        /// <summary>
        /// Configures the context to connect to a BasicSQL database.
        /// </summary>
        /// <param name="optionsBuilder">The builder being used to configure the context.</param>
        /// <param name="connectionString">The connection string of the database to connect to.</param>
        /// <param name="basicSqlOptionsAction">An optional action to allow additional BasicSQL specific configuration.</param>
        /// <returns>The options builder so that further configuration can be chained.</returns>
        public static DbContextOptionsBuilder UseBasicSql(
            this DbContextOptionsBuilder optionsBuilder,
            string connectionString,
            Action<BasicSqlDbContextOptionsBuilder>? basicSqlOptionsAction = null)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
            }

            var extension = GetOrCreateExtension(optionsBuilder).WithConnectionString(connectionString);
            ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

            ConfigureWarnings(optionsBuilder);

            basicSqlOptionsAction?.Invoke(new BasicSqlDbContextOptionsBuilder(optionsBuilder));

            return optionsBuilder;
        }

        /// <summary>
        /// Configures the context to connect to a BasicSQL database.
        /// </summary>
        /// <typeparam name="TContext">The type of context to be configured.</typeparam>
        /// <param name="optionsBuilder">The builder being used to configure the context.</param>
        /// <param name="connectionString">The connection string of the database to connect to.</param>
        /// <param name="basicSqlOptionsAction">An optional action to allow additional BasicSQL specific configuration.</param>
        /// <returns>The options builder so that further configuration can be chained.</returns>
        public static DbContextOptionsBuilder<TContext> UseBasicSql<TContext>(
            this DbContextOptionsBuilder<TContext> optionsBuilder,
            string connectionString,
            Action<BasicSqlDbContextOptionsBuilder>? basicSqlOptionsAction = null)
            where TContext : DbContext
        {
            return (DbContextOptionsBuilder<TContext>)UseBasicSql(
                (DbContextOptionsBuilder)optionsBuilder, connectionString, basicSqlOptionsAction);
        }

        private static BasicSqlOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.Options.FindExtension<BasicSqlOptionsExtension>()
                ?? new BasicSqlOptionsExtension();

        private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
        {
            // Configure any BasicSQL-specific warnings here
            // For now, we'll use the default configuration
        }
    }
}