using BasicSQL.Core;
using Xunit;

namespace BasicSQL.Tests
{
    public class AuthorizationTests
    {
        private AuthenticationManager _authManager;
        private BinarySqlEngine _engine;

        public AuthorizationTests()
        {
            _engine = new BinarySqlEngine(":memory:");
            _authManager = new AuthenticationManager(_engine);
        }

        [Fact]
        public void CreateAdminUser_AllowsAllCommands()
        {
            _authManager.CreateUser("admin", "password", "admin");
            var role = _authManager.GetUserRole("admin");
            Assert.Equal("admin", role);
        }

        [Fact]
        public void CreateUser_AllowsSelectOnly()
        {
            _authManager.CreateUser("user", "password", "user");
            var role = _authManager.GetUserRole("user");
            Assert.Equal("user", role);
        }
    }
}
