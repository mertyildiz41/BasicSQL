using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace BasicSQL.EntityFramework.Infrastructure
{
    /// <summary>
    /// BasicSQL-specific extension methods for DbContextOptionsBuilder.
    /// </summary>
    public class BasicSqlDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<BasicSqlDbContextOptionsBuilder, BasicSqlOptionsExtension>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BasicSqlDbContextOptionsBuilder"/> class.
        /// </summary>
        /// <param name="optionsBuilder">The options builder.</param>
        public BasicSqlDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
            : base(optionsBuilder)
        {
        }

        /// <summary>
        /// Configures the database file path for BasicSQL.
        /// </summary>
        /// <param name="databasePath">The path to the database file.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public BasicSqlDbContextOptionsBuilder DatabasePath(string databasePath)
        {
            return WithOption(e => ((BasicSqlOptionsExtension)e).WithDatabasePath(databasePath));
        }

        /// <summary>
        /// Configures the connection string for BasicSQL.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns>The same builder instance so that multiple calls can be chained.</returns>
        public BasicSqlDbContextOptionsBuilder ConnectionString(string connectionString)
        {
            return WithOption(e => (BasicSqlOptionsExtension)((BasicSqlOptionsExtension)e).WithConnectionString(connectionString));
        }
    }
}
