using BasicSQL.EntityFramework.Extensions;
using BasicSQL.EntityFramework.Infrastructure;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace BasicSQL.EntityFramework.Tests.Extensions
{
    public class BasicSqlDbContextOptionsExtensionsTests
    {
        [Fact]
        public void UseBasicSql_WithConnectionString_ShouldConfigureOptions()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
            var connectionString = "Data Source=/test/database";

            // Act
            optionsBuilder.UseBasicSql(connectionString);

            // Assert
            var options = optionsBuilder.Options;
            options.Should().NotBeNull();
            
            var extension = options.FindExtension<BasicSqlOptionsExtension>();
            extension.Should().NotBeNull();
            extension!.ConnectionString.Should().Be(connectionString);
        }

        [Fact]
        public void UseBasicSql_WithConnectionStringAndAction_ShouldConfigureOptions()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
            var connectionString = "Data Source=/test/database";
            var databasePath = "/custom/path";

            // Act
            optionsBuilder.UseBasicSql(connectionString, basicSqlOptions =>
            {
                basicSqlOptions.DatabasePath(databasePath);
            });

            // Assert
            var options = optionsBuilder.Options;
            var extension = options.FindExtension<BasicSqlOptionsExtension>();
            extension.Should().NotBeNull();
            extension!.ConnectionString.Should().Be(connectionString);
            extension.DatabasePath.Should().Be(databasePath);
        }

        [Fact]
        public void UseBasicSql_WithGenericDbContext_ShouldWork()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder();
            var connectionString = "Data Source=/test/database";

            // Act
            optionsBuilder.UseBasicSql(connectionString);

            // Assert
            var options = optionsBuilder.Options;
            var extension = options.FindExtension<BasicSqlOptionsExtension>();
            extension.Should().NotBeNull();
            extension!.ConnectionString.Should().Be(connectionString);
        }

        [Fact]
        public void UseBasicSql_ShouldReplaceExistingExtension()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
            var connectionString1 = "Data Source=/test/database1";
            var connectionString2 = "Data Source=/test/database2";

            // Act
            optionsBuilder.UseBasicSql(connectionString1);
            optionsBuilder.UseBasicSql(connectionString2);

            // Assert
            var options = optionsBuilder.Options;
            var extension = options.FindExtension<BasicSqlOptionsExtension>();
            extension.Should().NotBeNull();
            extension!.ConnectionString.Should().Be(connectionString2);
        }

        [Fact]
        public void UseBasicSql_WithNullConnectionString_ShouldThrow()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert
            var act = () => optionsBuilder.UseBasicSql(null!);
            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void UseBasicSql_WithEmptyConnectionString_ShouldThrow()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert
            var act = () => optionsBuilder.UseBasicSql("");
            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void UseBasicSql_WithWhitespaceConnectionString_ShouldThrow()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();

            // Act & Assert
            var act = () => optionsBuilder.UseBasicSql("   ");
            act.Should().Throw<ArgumentException>();
        }

        [Fact]
        public void UseBasicSql_WithValidConnectionString_ShouldNotThrow()
        {
            // Arrange
            var optionsBuilder = new DbContextOptionsBuilder<TestDbContext>();
            var connectionString = "Data Source=/valid/path";

            // Act & Assert
            var act = () => optionsBuilder.UseBasicSql(connectionString);
            act.Should().NotThrow();
        }
    }

    // Test DbContext for testing purposes
    public class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
        {
        }

        public DbSet<TestEntity> TestEntities { get; set; } = null!;
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public decimal Amount { get; set; }
    }
}
