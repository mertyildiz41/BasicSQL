using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using BasicSQL.EntityFramework.Diagnostics;
using BasicSQL.EntityFramework.Storage;
using BasicSQL.EntityFramework.Update;
using System;
using System.Linq;

namespace BasicSQL.EntityFramework.Infrastructure
{
    /// <summary>
    /// Service collection extensions for BasicSQL Entity Framework provider.
    /// </summary>
    public static class BasicSqlServiceCollectionExtensions
    {
        /// <summary>
        /// Adds the services required by the BasicSQL database provider for Entity Framework.
        /// </summary>
        /// <param name="serviceCollection">The service collection.</param>
        /// <returns>The service collection.</returns>
        public static IServiceCollection AddEntityFrameworkBasicSql(this IServiceCollection serviceCollection)
        {
            var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection)
                .TryAdd<LoggingDefinitions, BasicSqlLoggingDefinitions>()
                .TryAdd<IDatabaseProvider, DatabaseProvider<BasicSqlOptionsExtension>>()
                .TryAdd<IRelationalConnection, BasicSqlRelationalConnection>(p =>
                {
                    var options = p.GetRequiredService<IDbContextOptions>();
                    var ext = options.Extensions.OfType<BasicSqlOptionsExtension>().FirstOrDefault();
                    var connStr = ext?.ConnectionString ?? string.Empty;
                    
                    // Check if the connection string indicates socket mode
                    var isSocketMode = connStr.Contains("mode=socket", StringComparison.OrdinalIgnoreCase) ||
                                     connStr.Contains("host=", StringComparison.OrdinalIgnoreCase);
                    
                    if (isSocketMode)
                    {
                        return new BasicSQL.EntityFramework.Storage.BasicSqlSocketRelationalConnection(p.GetRequiredService<RelationalConnectionDependencies>());
                    }
                    else
                    {
                        return new BasicSQL.EntityFramework.Storage.BasicSqlRelationalConnection(p.GetRequiredService<RelationalConnectionDependencies>());
                    }
                })
                .TryAdd<ISqlGenerationHelper, BasicSqlSqlGenerationHelper>()
                .TryAdd<IRelationalTypeMappingSource, BasicSqlTypeMappingSource>()
                .TryAdd<IModificationCommandBatchFactory, BasicSqlModificationCommandBatchFactory>()
                .TryAdd<ICommandBatchPreparer, BasicSqlCommandBatchPreparer>()
                .TryAdd<IUpdateSqlGenerator, BasicSqlUpdateSqlGenerator>()
                .TryAdd<IRelationalDatabaseCreator, BasicSqlDatabaseCreator>()
                .TryAdd<IValueGeneratorSelector, BasicSqlValueGeneratorSelector>();

            builder.TryAddCoreServices();

            return serviceCollection;
        }
    }
}
