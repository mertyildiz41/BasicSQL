using System;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace BasicSQL.EntityFramework.Tests.Integration
{
    public class BasicSqlSocketIntegrationTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public class TestDbContext : DbContext
        {
            public DbSet<TestEntity> Entities { get; set; }
            protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            {
                // Use socket mode, default port 4162
                optionsBuilder.UseBasicSql("host=localhost;port=4162;mode=socket");
            }
        }

        [Fact]
        public async Task CanInsertAndQueryViaSocket()
        {
            using var db = new TestDbContext();
            db.Database.EnsureCreated();
            db.Entities.Add(new TestEntity { Name = "SocketTest" });
            await db.SaveChangesAsync();

            var entity = await db.Entities.FirstOrDefaultAsync(e => e.Name == "SocketTest");
            Assert.NotNull(entity);
            Assert.Equal("SocketTest", entity.Name);
        }
    }
}
