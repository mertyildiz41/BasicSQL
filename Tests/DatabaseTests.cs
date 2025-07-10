using BasicSQL.Core;
using Xunit;
using System.IO;

namespace BasicSQL.Tests
{
    public class DatabaseTests : IDisposable
    {
        private readonly string _baseTestDirectory = Path.Combine(Path.GetTempPath(), "BasicSQL_Tests");

        public DatabaseTests()
        {
            if (Directory.Exists(_baseTestDirectory))
            {
                Directory.Delete(_baseTestDirectory, true);
            }
            Directory.CreateDirectory(_baseTestDirectory);
        }

        public void Dispose()
        {
            if (Directory.Exists(_baseTestDirectory))
            {
                Directory.Delete(_baseTestDirectory, true);
            }
        }

        [Fact]
        public void CreateDatabase_ShouldCreateDirectory()
        {
            // Arrange
            var engine = new BinarySqlEngine(_baseTestDirectory);

            // Act
            var result = engine.Execute("CREATE DATABASE my_test_db");

            // Assert
            Assert.True(result.Success);
            var dbPath = Path.Combine(_baseTestDirectory, "my_test_db");
            Assert.True(Directory.Exists(dbPath));
        }

        [Fact]
        public void ShowDatabases_ShouldListAllDatabases()
        {
            // Arrange
            var engine = new BinarySqlEngine(_baseTestDirectory);
            engine.Execute("CREATE DATABASE db1");
            engine.Execute("CREATE DATABASE db2");

            // Act
            var result = engine.Execute("SHOW DATABASES");

            // Assert
            Assert.True(result.Success);
            Assert.Equal(3, result.Databases.Count); // Includes 'default'
            Assert.Contains("default", result.Databases);
            Assert.Contains("db1", result.Databases);
            Assert.Contains("db2", result.Databases);
        }

        [Fact]
        public void UseDatabase_ShouldSwitchContextAndLoadTables()
        {
            // Arrange
            var engine = new BinarySqlEngine(_baseTestDirectory);
            engine.Execute("CREATE DATABASE db1");
            engine.Execute("USE db1");
            engine.Execute("CREATE TABLE t1 (id INT)");
            engine.Execute("USE default");
            engine.Execute("CREATE TABLE t2 (id INT)");

            // Act
            var result1 = engine.Execute("SHOW TABLES");
            var result2 = engine.Execute("USE db1");
            var result3 = engine.Execute("SHOW TABLES");

            // Assert
            Assert.True(result1.Success);
            Assert.Single(result1.Tables);
            Assert.Equal("t2", result1.Tables[0]);

            Assert.True(result2.Success);
            Assert.Equal("Switched to database 'db1'.", result2.Message);

            Assert.True(result3.Success);
            Assert.Single(result3.Tables);
            Assert.Equal("t1", result3.Tables[0]);
        }

        [Fact]
        public void DropDatabase_ShouldRemoveDirectoryAndSwitchToDefault()
        {
            // Arrange
            var engine = new BinarySqlEngine(_baseTestDirectory);
            engine.Execute("CREATE DATABASE db_to_drop");
            engine.Execute("USE db_to_drop");

            // Act
            var result = engine.Execute("DROP DATABASE db_to_drop");

            // Assert
            Assert.True(result.Success);
            var dbPath = Path.Combine(_baseTestDirectory, "db_to_drop");
            Assert.False(Directory.Exists(dbPath));

            // Check if we switched back to default
            var showTablesResult = engine.Execute("SHOW TABLES");
            Assert.True(showTablesResult.Success);
        }
    }
}
