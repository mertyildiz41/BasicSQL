using System;
using System.IO;
using System.Linq;
using Xunit;
using BasicSQL.Core;
using BasicSQL.Models;

namespace BasicSQL.Tests
{
    public class SqlEngineTests : IDisposable
    {
        private readonly BinarySqlEngine _engine;
        private readonly string _testDataDirectory;

        public SqlEngineTests()
        {
            _testDataDirectory = Path.Combine(Path.GetTempPath(), "test_sql_" + Guid.NewGuid().ToString("N"));
            _engine = new BinarySqlEngine(_testDataDirectory);
        }

        [Fact]
        public void CreateTable_ShouldCreateTableSuccessfully()
        {
            // Arrange
            var sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";

            // Act
            var result = _engine.Execute(sql);

            // Assert
            Assert.True(result.Success);
            Assert.Contains("created successfully", result.Message);
        }

        [Fact]
        public void CreateTable_WithAutoIncrement_ShouldCreateTableSuccessfully()
        {
            // Arrange
            var sql = "CREATE TABLE users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT)";

            // Act
            var result = _engine.Execute(sql);

            // Assert
            Assert.True(result.Success);
            Assert.Contains("created successfully", result.Message);
        }

        [Fact]
        public void Insert_BasicData_ShouldInsertSuccessfully()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";

            // Act
            var createResult = _engine.Execute(createSql);
            var insertResult = _engine.Execute(insertSql);

            // Assert
            Assert.True(createResult.Success);
            Assert.True(insertResult.Success);
            Assert.Equal(1, insertResult.RowsAffected);
        }

        [Fact]
        public void Insert_WithAutoIncrement_ShouldGenerateId()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insertSql = "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')";

            // Act
            var createResult = _engine.Execute(createSql);
            var insertResult = _engine.Execute(insertSql);

            // Assert
            Assert.True(createResult.Success);
            Assert.True(insertResult.Success);
            Assert.Equal(1, insertResult.RowsAffected);
        }

        [Fact]
        public void Insert_MultipleAutoIncrement_ShouldIncrementSequentially()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insert1Sql = "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')";
            var insert2Sql = "INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com')";
            var selectSql = "SELECT * FROM users";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insert1Sql);
            _engine.Execute(insert2Sql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(selectResult.Success);
            Assert.Equal(2, selectResult.Rows.Count);
            
            // Check that IDs are sequential
            var firstId = Convert.ToInt32(selectResult.Rows[0]["id"]);
            var secondId = Convert.ToInt32(selectResult.Rows[1]["id"]);
            Assert.Equal(secondId, firstId + 1);
        }

        [Fact]
        public void Select_FromExistingTable_ShouldReturnData()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";
            var selectSql = "SELECT * FROM users";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var result = _engine.Execute(selectSql);

            // Assert
            Assert.True(result.Success);
            Assert.Single(result.Rows);
            Assert.Equal("John Doe", result.Rows[0]["name"]);
            Assert.Equal("john@example.com", result.Rows[0]["email"]);
        }

        [Fact]
        public void Select_WithWhere_ShouldFilterResults()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insert1Sql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";
            var insert2Sql = "INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane@example.com')";
            var selectSql = "SELECT * FROM users WHERE name = 'John Doe'";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insert1Sql);
            _engine.Execute(insert2Sql);
            var result = _engine.Execute(selectSql);

            // Assert
            Assert.True(result.Success);
            Assert.Single(result.Rows);
            Assert.Equal("John Doe", result.Rows[0]["name"]);
        }

        [Fact]
        public void Update_ExistingRecord_ShouldUpdateSuccessfully()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";
            var updateSql = "UPDATE users SET email = 'newemail@example.com' WHERE id = 1";
            var selectSql = "SELECT * FROM users WHERE id = 1";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var updateResult = _engine.Execute(updateSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(updateResult.Success);
            Assert.Equal(1, updateResult.RowsAffected);
            Assert.True(selectResult.Success);
            Assert.Single(selectResult.Rows);
            Assert.Equal("newemail@example.com", selectResult.Rows[0]["email"]);
        }

        [Fact]
        public void Delete_ExistingRecord_ShouldDeleteSuccessfully()
        {
            // Arrange
            var createSql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";
            var insertSql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";
            var deleteSql = "DELETE FROM users WHERE id = 1";
            var selectSql = "SELECT * FROM users";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var deleteResult = _engine.Execute(deleteSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(deleteResult.Success);
            Assert.Equal(1, deleteResult.RowsAffected);
            Assert.True(selectResult.Success);
            Assert.Empty(selectResult.Rows);
        }

        [Fact]
        public void Execute_InvalidSql_ShouldReturnError()
        {
            // Arrange
            var invalidSql = "INVALID SQL STATEMENT";

            // Act
            var result = _engine.Execute(invalidSql);

            // Assert
            Assert.False(result.Success);
            Assert.NotNull(result.ErrorMessage);
            Assert.NotEmpty(result.ErrorMessage);
        }

        [Fact]
        public void Execute_EmptyString_ShouldReturnError()
        {
            // Arrange
            var emptySql = "";

            // Act
            var result = _engine.Execute(emptySql);

            // Assert
            Assert.False(result.Success);
            Assert.Equal("Empty SQL statement", result.ErrorMessage);
        }

        [Fact]
        public void ShowTables_ShouldListCreatedTables()
        {
            // Arrange
            var createSql1 = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
            var createSql2 = "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)";
            var showTablesSql = ".tables";

            // Act
            _engine.Execute(createSql1);
            _engine.Execute(createSql2);
            var result = _engine.Execute(showTablesSql);

            // Assert
            Assert.True(result.Success);
            Assert.Contains("users", result.Tables);
            Assert.Contains("products", result.Tables);
        }

        public void Dispose()
        {
            // Clean up test data directory
            if (Directory.Exists(_testDataDirectory))
            {
                Directory.Delete(_testDataDirectory, true);
            }
        }
    }
}
