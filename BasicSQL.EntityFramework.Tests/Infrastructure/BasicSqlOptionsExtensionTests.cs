using BasicSQL.EntityFramework.Infrastructure;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BasicSQL.EntityFramework.Tests.Infrastructure
{
    public class BasicSqlOptionsExtensionTests
    {
        [Fact]
        public void Constructor_ShouldCreateDefaultInstance()
        {
            // Act
            var extension = new BasicSqlOptionsExtension();

            // Assert
            extension.Should().NotBeNull();
            extension.DatabasePath.Should().BeNull();
            extension.ConnectionString.Should().BeNull();
        }

        [Fact]
        public void WithDatabasePath_ShouldSetDatabasePath()
        {
            // Arrange
            var extension = new BasicSqlOptionsExtension();
            const string testPath = "/test/path";

            // Act
            var result = extension.WithDatabasePath(testPath);

            // Assert
            result.DatabasePath.Should().Be(testPath);
            result.Should().NotBeSameAs(extension);
        }

        [Fact]
        public void WithConnectionString_ShouldSetConnectionString()
        {
            // Arrange
            var extension = new BasicSqlOptionsExtension();
            const string testConnectionString = "test-connection-string";

            // Act
            var result = extension.WithConnectionString(testConnectionString);

            // Assert
            result.Should().NotBeSameAs(extension);
            result.ConnectionString.Should().Be(testConnectionString);
            extension.ConnectionString.Should().BeNull();
        }

        [Fact]
        public void ApplyServices_ShouldRegisterBasicSqlServices()
        {
            // Arrange
            var extension = new BasicSqlOptionsExtension();
            var services = new ServiceCollection();

            // Act
            extension.ApplyServices(services);

            // Assert
            services.Should().NotBeEmpty();
        }

        [Fact]
        public void Info_IsDatabaseProvider_ShouldReturnTrue()
        {
            // Arrange
            var extension = new BasicSqlOptionsExtension();

            // Act & Assert
            extension.Info.IsDatabaseProvider.Should().BeTrue();
        }
    }
}
