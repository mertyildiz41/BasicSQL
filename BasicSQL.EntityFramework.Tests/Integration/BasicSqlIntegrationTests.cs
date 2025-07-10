using BasicSQL.EntityFramework.Extensions;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Xunit;
using Xunit.Abstractions;

namespace BasicSQL.EntityFramework.Tests.Integration
{
    public class BasicSqlIntegrationTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _testDbPath;
        private readonly TestDbContext _context;

        public BasicSqlIntegrationTests(ITestOutputHelper output)
        {
            _output = output;
            _testDbPath = Path.Combine(Path.GetTempPath(), $"test_db_{Guid.NewGuid():N}");
            Directory.CreateDirectory(_testDbPath);
            Console.WriteLine($"Using test database at: {_testDbPath}");

            var options = new DbContextOptionsBuilder<TestDbContext>()
                .UseBasicSql($"Data Source={_testDbPath}")
                .UseLoggerFactory(LoggerFactory.Create(builder => 
                    builder.AddConsole().SetMinimumLevel(LogLevel.Debug)))
                .EnableSensitiveDataLogging()
                .Options;

            _context = new TestDbContext(options);
        }

        [Fact]
        public void DbContext_ShouldBeConfiguredWithBasicSql()
        {
            // Arrange & Act
            var database = _context.Database;

            // Assert
            database.Should().NotBeNull();
            database.ProviderName.Should().Contain("BasicSQL", "The provider should be BasicSQL");
        }

        [Fact]
        public async Task CanCreateDatabase_ShouldSucceed()
        {
            // Act
            var canConnect = await _context.Database.CanConnectAsync();

            // Assert
            canConnect.Should().BeTrue("Should be able to connect to BasicSQL database");
        }

        [Fact]
        public async Task EnsureCreated_ShouldCreateDatabaseStructure()
        {
            // Act
            var created = await _context.Database.EnsureCreatedAsync();

            // Assert
            created.Should().BeTrue("Database should be created successfully");
            
            // Verify we can still connect after creation
            var canConnect = await _context.Database.CanConnectAsync();
            canConnect.Should().BeTrue("Should be able to connect after database creation");
        }

        [Fact]
        public void DbSet_ShouldBeAccessible()
        {
            // Act & Assert
            _context.TestEntities.Should().NotBeNull("DbSet should be accessible");
        }

        [Fact]
        public async Task Add_ShouldWorkWithAutoIncrement()
        {
            // Arrange
            await _context.Database.EnsureCreatedAsync();
            
            var entity = new TestEntity
            {
                // Don't set Id - let auto-increment handle it
                Name = "Test Entity",
                CreatedAt = DateTime.Now,
                Amount = 123.45m
            };

            // Act
            _context.TestEntities.Add(entity);
            
            var saveResult = await _context.SaveChangesAsync();

            // Assert
            saveResult.Should().Be(1, "One entity should be saved");
            entity.Id.Should().BeGreaterThan(0, "Entity should have an auto-generated ID");

            // Verify the entity was actually saved
            var savedEntity = await _context.TestEntities.FindAsync(entity.Id);
            savedEntity.Should().NotBeNull();
            savedEntity!.Name.Should().Be("Test Entity");
            savedEntity.Amount.Should().Be(123.45m);
        }

        [Fact]
        public async Task Query_ShouldReturnCorrectResults()
        {
            // Arrange
            await _context.Database.EnsureCreatedAsync();
            
            var entities = new[]
            {
                new TestEntity { Name = "Entity 1", CreatedAt = DateTime.Now.AddDays(-1), Amount = 100m },
                new TestEntity { Name = "Entity 2", CreatedAt = DateTime.Now, Amount = 200m },
                new TestEntity { Name = "Entity 3", CreatedAt = DateTime.Now.AddDays(1), Amount = 300m }
            };

            _context.TestEntities.AddRange(entities);
            await _context.SaveChangesAsync();

            // Act
            var results = await _context.TestEntities
                .Where(e => e.Amount > 150m)
                .OrderBy(e => e.Name)
                .ToListAsync();

            // Assert
            results.Should().HaveCount(2);
            results[0].Name.Should().Be("Entity 2");
            results[1].Name.Should().Be("Entity 3");
        }

        [Fact]
        public async Task Update_ShouldModifyEntity()
        {
            // Arrange
            await _context.Database.EnsureCreatedAsync();
            
            var entity = new TestEntity
            {
                Name = "Original Name",
                CreatedAt = DateTime.Now,
                Amount = 100m
            };

            _context.TestEntities.Add(entity);
            await _context.SaveChangesAsync();

            // Act
            entity.Name = "Updated Name";
            entity.Amount = 200m;
            var updateResult = await _context.SaveChangesAsync();

            // Assert
            updateResult.Should().Be(1, "One entity should be updated");

            // Verify the update
            var updatedEntity = await _context.TestEntities.FindAsync(entity.Id);
            updatedEntity!.Name.Should().Be("Updated Name");
            updatedEntity.Amount.Should().Be(200m);
        }

        [Fact]
        public async Task Delete_ShouldRemoveEntity()
        {
            // Arrange
            await _context.Database.EnsureCreatedAsync();
            
            var entity = new TestEntity
            {
                Name = "To Be Deleted",
                CreatedAt = DateTime.Now,
                Amount = 100m
            };

            _context.TestEntities.Add(entity);
            await _context.SaveChangesAsync();
            var entityId = entity.Id;

            // Act
            _context.TestEntities.Remove(entity);
            var deleteResult = await _context.SaveChangesAsync();

            // Assert
            deleteResult.Should().Be(1, "One entity should be deleted");

            // Verify the deletion
            var deletedEntity = await _context.TestEntities.FindAsync(entityId);
            deletedEntity.Should().BeNull("Entity should be deleted");
        }

        [Fact]
        public async Task ComplexQuery_WithMultipleConditions_ShouldWork()
        {
            // Arrange
            await _context.Database.EnsureCreatedAsync();
            
            var entities = new[]
            {
                new TestEntity { Name = "Alpha", CreatedAt = DateTime.Now.AddDays(-5), Amount = 50m },
                new TestEntity { Name = "Beta", CreatedAt = DateTime.Now.AddDays(-3), Amount = 150m },
                new TestEntity { Name = "Gamma", CreatedAt = DateTime.Now.AddDays(-1), Amount = 250m },
                new TestEntity { Name = "Delta", CreatedAt = DateTime.Now, Amount = 350m }
            };

            _context.TestEntities.AddRange(entities);
            await _context.SaveChangesAsync();

            // Act
            var results = await _context.TestEntities
                .Where(e => e.Amount >= 100m && e.Name.Contains("a"))
                .OrderByDescending(e => e.Amount)
                .Select(e => new { e.Name, e.Amount })
                .ToListAsync();

            // Assert
            results.Should().HaveCount(2);
            results[0].Name.Should().Be("Gamma");
            results[0].Amount.Should().Be(250m);
            results[1].Name.Should().Be("Beta");
            results[1].Amount.Should().Be(150m);
        }

        public void Dispose()
        {
            _context?.Dispose();
            
            if (Directory.Exists(_testDbPath))
            {
                try
                {
                    Directory.Delete(_testDbPath, true);
                }
                catch
                {
                    // Ignore cleanup errors in tests
                }
            }
        }
    }

    // Test DbContext for integration testing
    public class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
        {
        }

        public DbSet<TestEntity> TestEntities { get; set; } = null!;
    }

    public class TestEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public decimal Amount { get; set; }
    }
}
