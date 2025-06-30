using Microsoft.EntityFrameworkCore.Storage;

namespace BasicSQL.EntityFramework.Storage;

/// <summary>
/// BasicSQL implementation of SQL generation helper.
/// </summary>
public class BasicSqlSqlGenerationHelper : RelationalSqlGenerationHelper
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BasicSqlSqlGenerationHelper"/> class.
    /// </summary>
    /// <param name="dependencies">The dependencies.</param>
    public BasicSqlSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// Gets the SQL batch terminator used in BasicSQL.
    /// </summary>
    public override string BatchTerminator => ";";

    /// <summary>
    /// Gets the statement terminator used in BasicSQL.
    /// </summary>
    public override string StatementTerminator => ";";

    /// <summary>
    /// Gets the delimiter for identifier names in BasicSQL.
    /// </summary>
    /// <param name="identifier">The identifier to delimit.</param>
    /// <returns>The delimited identifier.</returns>
    public override string DelimitIdentifier(string identifier)
        => $"[{EscapeIdentifier(identifier)}]";

    /// <summary>
    /// Gets the delimiter for identifier names in BasicSQL.
    /// </summary>
    /// <param name="name">The name part of the identifier.</param>
    /// <param name="schema">The schema part of the identifier.</param>
    /// <returns>The delimited identifier.</returns>
    public override string DelimitIdentifier(string name, string? schema)
        => schema != null
            ? $"[{EscapeIdentifier(schema)}].[{EscapeIdentifier(name)}]"
            : DelimitIdentifier(name);

    /// <summary>
    /// Escapes special characters in an identifier.
    /// </summary>
    /// <param name="identifier">The identifier to escape.</param>
    /// <returns>The escaped identifier.</returns>
    public override string EscapeIdentifier(string identifier)
        => identifier.Replace("]", "]]");
}
