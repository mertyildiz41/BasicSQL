using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Storage;

namespace BasicSQL.EntityFramework.Update;

/// <summary>
/// BasicSQL implementation of modification command batch factory.
/// </summary>
public class BasicSqlModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    /// <summary>
    /// Initializes a new instance of the <see cref="BasicSqlModificationCommandBatchFactory"/> class.
    /// </summary>
    /// <param name="dependencies">The dependencies.</param>
    public BasicSqlModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    /// <summary>
    /// Creates a new modification command batch.
    /// </summary>
    /// <returns>The modification command batch.</returns>
    public ModificationCommandBatch Create()
    {
        return new BasicSqlModificationCommandBatch(_dependencies);
    }
}
