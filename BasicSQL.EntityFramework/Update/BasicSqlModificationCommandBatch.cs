using Microsoft.EntityFrameworkCore.Update;

namespace BasicSQL.EntityFramework.Update
{
    public class BasicSqlModificationCommandBatch : SingularModificationCommandBatch
    {
        public BasicSqlModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
            : base(dependencies)
        {
        }

        protected override ModificationCommand CreateModificationCommand(
            ModificationCommandParameters modificationCommandParameters)
            => new BasicSqlModificationCommand(modificationCommandParameters);
    }
}
