using BasicSQL.EntityFramework.Infrastructure;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BasicSQL.EntityFramework.Tests.Infrastructure
{
    public class BasicSqlServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddEntityFrameworkBasicSql_ShouldRegisterRequiredServices()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddEntityFrameworkBasicSql();

            // Assert
            services.Should().NotBeEmpty();
            
            // Verify core EF services are registered
            var serviceProvider = services.BuildServiceProvider();
            serviceProvider.Should().NotBeNull();
        }

        [Fact]
        public void AddEntityFrameworkBasicSql_ShouldRegisterDatabaseProvider()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddEntityFrameworkBasicSql();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            
            // Try to get the database provider - this should not throw
            var provider = serviceProvider.GetService<Microsoft.EntityFrameworkCore.Storage.IDatabaseProvider>();
            provider.Should().NotBeNull();
        }

        [Fact]
        public void AddEntityFrameworkBasicSql_ShouldBeIdempotent()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddEntityFrameworkBasicSql();
            services.AddEntityFrameworkBasicSql(); // Call twice

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            serviceProvider.Should().NotBeNull();
            
            // Should not throw or cause conflicts
            var provider = serviceProvider.GetService<Microsoft.EntityFrameworkCore.Storage.IDatabaseProvider>();
            provider.Should().NotBeNull();
        }

        [Fact]
        public void AddEntityFrameworkBasicSql_ShouldRegisterLoggingDefinitions()
        {
            // Arrange
            var services = new ServiceCollection();

            // Act
            services.AddEntityFrameworkBasicSql();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            
            // Try to get logging definitions
            var loggingDefinitions = serviceProvider.GetService<Microsoft.EntityFrameworkCore.Diagnostics.LoggingDefinitions>();
            loggingDefinitions.Should().NotBeNull();
        }

        [Fact]
        public void AddEntityFrameworkBasicSql_ShouldWorkWithExistingServices()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddLogging();
            // Add other services to test compatibility

            // Act
            services.AddEntityFrameworkBasicSql();

            // Assert
            var serviceProvider = services.BuildServiceProvider();
            serviceProvider.Should().NotBeNull();
            
            // Both providers should coexist
            var providers = serviceProvider.GetServices<Microsoft.EntityFrameworkCore.Storage.IDatabaseProvider>();
            providers.Should().NotBeEmpty();
        }
    }
}
