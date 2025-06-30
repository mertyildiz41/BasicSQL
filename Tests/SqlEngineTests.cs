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
            Assert.True(createResult.Success, $"Create failed: {createResult.Message}");
            Assert.True(insertResult.Success, $"Insert failed: {insertResult.Message}");
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

        [Fact]
        public void DateTime_CreateTableAndInsert_ShouldWorkCorrectly()
        {
            // Arrange
            var createSql = "CREATE TABLE events (id INTEGER, name TEXT, event_date DATETIME, priority INTEGER)";
            var insertSql = "INSERT INTO events (id, name, event_date, priority) VALUES (1, 'Test Event', '2024-01-15 09:00:00', 1)";

            // Act
            var createResult = _engine.Execute(createSql);
            var insertResult = _engine.Execute(insertSql);

            // Assert
            Assert.True(createResult.Success, $"Create failed: {createResult.Message}");
            Assert.True(insertResult.Success, $"Insert failed: {insertResult.Message}");
        }

        [Fact]
        public void DateTime_SelectAfterInsert_ShouldReturnCorrectData()
        {
            // Arrange
            var createSql = "CREATE TABLE events (id INTEGER, name TEXT, event_date DATETIME)";
            var insertSql = "INSERT INTO events (id, name, event_date) VALUES (1, 'Test Event', '2024-01-15 09:00:00')";
            var selectSql = "SELECT * FROM events";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            Assert.Equal(DateTime.Parse("2024-01-15 09:00:00"), row["event_date"]);
        }

        [Fact]
        public void DateTime_UpdateOperation_ShouldWorkCorrectly()
        {
            // Arrange
            var createSql = "CREATE TABLE events (id INTEGER, event_date DATETIME)";
            var insertSql = "INSERT INTO events (id, event_date) VALUES (1, '2024-01-15 09:00:00')";
            var updateSql = "UPDATE events SET event_date = '2024-02-15 10:30:00' WHERE id = 1";
            var selectSql = "SELECT * FROM events WHERE id = 1";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var updateResult = _engine.Execute(updateSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(updateResult.Success, $"Update failed: {updateResult.Message}");
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            Assert.Equal(DateTime.Parse("2024-02-15 10:30:00"), row["event_date"]);
        }

        [Fact]
        public void Decimal_CreateTableAndInsert_ShouldWorkCorrectly()
        {
            // Arrange
            var createSql = "CREATE TABLE products (id INTEGER, name TEXT, price DECIMAL, cost DECIMAL)";
            var insertSql = "INSERT INTO products (id, name, price, cost) VALUES (1, 'Laptop', 999.99, 650.50)";

            // Act
            var createResult = _engine.Execute(createSql);
            var insertResult = _engine.Execute(insertSql);

            // Assert
            Assert.True(createResult.Success, $"Create failed: {createResult.Message}");
            Assert.True(insertResult.Success, $"Insert failed: {insertResult.Message}");
        }

        [Fact]
        public void Decimal_SelectAfterInsert_ShouldReturnCorrectData()
        {
            // Arrange
            var createSql = "CREATE TABLE products (id INTEGER, name TEXT, price DECIMAL, cost DECIMAL)";
            var insertSql = "INSERT INTO products (id, name, price, cost) VALUES (1, 'Laptop', 999.99, 650.50)";
            var selectSql = "SELECT * FROM products";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            Assert.Equal(999.99m, row["price"]);
            Assert.Equal(650.50m, row["cost"]);
        }

        [Fact]
        public void Decimal_HighPrecisionValues_ShouldMaintainPrecision()
        {
            // Arrange
            var createSql = "CREATE TABLE precision_test (id INTEGER, value DECIMAL)";
            var highPrecisionValue = "123456789.123456789";
            var insertSql = $"INSERT INTO precision_test (id, value) VALUES (1, {highPrecisionValue})";
            var selectSql = "SELECT * FROM precision_test";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            var retrievedValue = (decimal)row["value"];
            Assert.Equal(decimal.Parse(highPrecisionValue), retrievedValue);
        }

        [Fact]
        public void Decimal_NegativeValues_ShouldWorkCorrectly()
        {
            // Arrange
            var createSql = "CREATE TABLE accounting (id INTEGER, balance DECIMAL)";
            var insertSql = "INSERT INTO accounting (id, balance) VALUES (1, -123.45)";
            var selectSql = "SELECT * FROM accounting";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            Assert.Equal(-123.45m, row["balance"]);
        }

        [Fact]
        public void Decimal_UpdateOperation_ShouldWorkCorrectly()
        {
            // Arrange
            var createSql = "CREATE TABLE products (id INTEGER, price DECIMAL)";
            var insertSql = "INSERT INTO products (id, price) VALUES (1, 100.00)";
            var updateSql = "UPDATE products SET price = 150.75 WHERE id = 1";
            var selectSql = "SELECT * FROM products WHERE id = 1";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var updateResult = _engine.Execute(updateSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(updateResult.Success, $"Update failed: {updateResult.Message}");
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            Assert.Equal(150.75m, row["price"]);
        }

        [Fact]
        public void Mixed_DateTimeAndDecimal_ShouldWorkTogether()
        {
            // Arrange
            var createSql = "CREATE TABLE transactions (id INTEGER, amount DECIMAL, transaction_date DATETIME, description TEXT)";
            var insertSql = "INSERT INTO transactions (id, amount, transaction_date, description) VALUES (1, 1234.56, '2024-01-15 14:30:00', 'Test Transaction')";
            var selectSql = "SELECT * FROM transactions";

            // Act
            _engine.Execute(createSql);
            _engine.Execute(insertSql);
            var selectResult = _engine.Execute(selectSql);

            // Assert
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Single(selectResult.Rows);
            
            var row = selectResult.Rows.First();
            Assert.Equal(1234.56m, row["amount"]);
            Assert.Equal(DateTime.Parse("2024-01-15 14:30:00"), row["transaction_date"]);
            Assert.Equal("Test Transaction", row["description"]);
        }

        [Fact]
        public void BinaryStorage_LargeDataSet_WithDateTimeAndDecimal_ShouldPerformWell()
        {
            // Arrange
            var createSql = "CREATE TABLE performance_test (id INTEGER, price DECIMAL, created_date DATETIME, name TEXT)";
            _engine.Execute(createSql);

            // Act - Insert 1000 records
            var insertCount = 1000;
            for (int i = 1; i <= insertCount; i++)
            {
                var price = (decimal)(i * 1.5 + 0.99);
                var date = DateTime.Now.AddDays(-i).ToString("yyyy-MM-dd HH:mm:ss");
                var insertSql = $"INSERT INTO performance_test (id, price, created_date, name) VALUES ({i}, {price}, '{date}', 'Item {i}')";
                var result = _engine.Execute(insertSql);
                Assert.True(result.Success, $"Insert {i} failed: {result.Message}");
            }

            // Assert - Verify data integrity
            var selectSql = "SELECT * FROM performance_test";
            var selectResult = _engine.Execute(selectSql);
            
            Assert.True(selectResult.Success);
            Assert.NotNull(selectResult.Rows);
            Assert.Equal(insertCount, selectResult.Rows.Count());
            
            // Verify first and last records have correct data types
            var firstRow = selectResult.Rows.First();
            var lastRow = selectResult.Rows.Last();
            
            Assert.IsType<decimal>(firstRow["price"]);
            Assert.IsType<DateTime>(firstRow["created_date"]);
            Assert.IsType<decimal>(lastRow["price"]);
            Assert.IsType<DateTime>(lastRow["created_date"]);
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
