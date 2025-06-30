using Microsoft.EntityFrameworkCore.Diagnostics;

namespace BasicSQL.EntityFramework.Diagnostics
{
    /// <summary>
    /// BasicSQL-specific logging definitions for Entity Framework Core.
    /// </summary>
    public class BasicSqlLoggingDefinitions : LoggingDefinitions
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BasicSqlLoggingDefinitions"/> class.
        /// </summary>
        public BasicSqlLoggingDefinitions()
        {
        }
    }
}
