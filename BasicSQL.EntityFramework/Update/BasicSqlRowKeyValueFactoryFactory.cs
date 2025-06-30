using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace BasicSQL.EntityFramework.Update
{
    /// <summary>
    /// Row key value factory selector for BasicSQL.
    /// </summary>
    public class BasicSqlRowKeyValueFactoryFactory : IRowKeyValueFactoryFactory
    {
        public virtual IRowKeyValueFactory Create(IKey key)
        {
            return new BasicSqlRowKeyValueFactory(key);
        }

        public virtual IRowKeyValueFactory Create(IUniqueConstraint constraint)
        {
            // For unique constraints, we can use a simpler implementation
            // since they're not typically auto-generated
            return new SimpleRowKeyValueFactory<object>(constraint);
        }
    }
}
