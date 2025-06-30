using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using System.Globalization;

namespace BasicSQL.EntityFramework.Infrastructure
{
    /// <summary>
    /// BasicSQL-specific options extension for Entity Framework Core.
    /// </summary>
    public class BasicSqlOptionsExtension : RelationalOptionsExtension
    {
        private DbContextOptionsExtensionInfo? _info;
        private string? _databasePath;

        /// <summary>
        /// Initializes a new instance of the <see cref="BasicSqlOptionsExtension"/> class.
        /// </summary>
        public BasicSqlOptionsExtension()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BasicSqlOptionsExtension"/> class.
        /// </summary>
        /// <param name="copyFrom">The extension to copy from.</param>
        protected BasicSqlOptionsExtension(BasicSqlOptionsExtension copyFrom)
            : base(copyFrom)
        {
            _databasePath = copyFrom._databasePath;
        }

        /// <summary>
        /// Gets the database path.
        /// </summary>
        public virtual string? DatabasePath => _databasePath;

        /// <summary>
        /// Gets information/metadata about the extension.
        /// </summary>
        public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

        /// <summary>
        /// Creates a clone of this extension with the specified database path.
        /// </summary>
        /// <param name="databasePath">The database path.</param>
        /// <returns>A new extension instance.</returns>
        public virtual BasicSqlOptionsExtension WithDatabasePath(string databasePath)
        {
            var clone = (BasicSqlOptionsExtension)Clone();
            clone._databasePath = databasePath;
            return clone;
        }

        /// <summary>
        /// Creates a clone of this extension.
        /// </summary>
        /// <returns>A new extension instance.</returns>
        protected override RelationalOptionsExtension Clone()
            => new BasicSqlOptionsExtension(this);

        /// <summary>
        /// Applies the services required by this extension.
        /// </summary>
        /// <param name="services">The service collection.</param>
        public override void ApplyServices(IServiceCollection services)
            => services.AddEntityFrameworkBasicSql();

        private sealed class ExtensionInfo : RelationalExtensionInfo
        {
            private string? _logFragment;

            public ExtensionInfo(IDbContextOptionsExtension extension)
                : base(extension)
            {
            }

            private new BasicSqlOptionsExtension Extension
                => (BasicSqlOptionsExtension)base.Extension;

            public override bool IsDatabaseProvider => true;

            public override string LogFragment
            {
                get
                {
                    if (_logFragment == null)
                    {
                        var builder = new System.Text.StringBuilder();

                        builder.Append("DatabasePath=").Append(Extension.DatabasePath ?? "<null>");

                        _logFragment = builder.ToString();
                    }

                    return _logFragment;
                }
            }

            public override int GetServiceProviderHashCode()
            {
                var hashCode = base.GetServiceProviderHashCode();
                hashCode = (hashCode * 3) ^ (Extension.DatabasePath?.GetHashCode() ?? 0);
                return hashCode;
            }

            public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
                => other is ExtensionInfo otherInfo && string.Equals(Extension.DatabasePath, otherInfo.Extension.DatabasePath);

            public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            {
                debugInfo["BasicSQL:" + nameof(Extension.DatabasePath)]
                    = (Extension.DatabasePath?.GetHashCode() ?? 0).ToString(CultureInfo.InvariantCulture);
            }
        }
    }
}
