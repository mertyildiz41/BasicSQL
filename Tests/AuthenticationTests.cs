using BasicSQL.Core;
using Xunit;
using System.IO;

namespace BasicSQL.Tests
{
    public class AuthenticationTests : IDisposable
    {
        private readonly BinarySqlEngine _engine;
        private readonly AuthenticationManager _authManager;
        private readonly string _testDataDirectory;

        public AuthenticationTests()
        {
            _testDataDirectory = Path.Combine(Path.GetTempPath(), "test_auth_" + System.Guid.NewGuid().ToString("N"));
            _engine = new BinarySqlEngine(_testDataDirectory);
            _authManager = new AuthenticationManager(_engine);
        }

        [Fact]
        public void CreateUser_ShouldSucceed_WithValidCredentials()
        {
            // Act
            var result = _authManager.CreateUser("testuser", "password123");

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void CreateUser_ShouldFail_WithEmptyUsername()
        {
            // Act
            var result = _authManager.CreateUser("", "password123");

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void Authenticate_ShouldSucceed_WithCorrectCredentials()
        {
            // Arrange
            _authManager.CreateUser("testuser", "password123");

            // Act
            var result = _authManager.Authenticate("testuser", "password123");

            // Assert
            Assert.True(result);
        }

        [Fact]
        public void Authenticate_ShouldFail_WithIncorrectPassword()
        {
            // Arrange
            _authManager.CreateUser("testuser", "password123");

            // Act
            var result = _authManager.Authenticate("testuser", "wrongpassword");

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void Authenticate_ShouldFail_ForNonExistentUser()
        {
            // Act
            var result = _authManager.Authenticate("nonexistent", "password123");

            // Assert
            Assert.False(result);
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDataDirectory))
            {
                Directory.Delete(_testDataDirectory, true);
            }
        }
    }
}
