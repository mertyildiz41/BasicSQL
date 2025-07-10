using System;
using System.Linq;
using Xunit;
using BasicSQL.Parsers;
using BasicSQL.Models;

namespace BasicSQL.Tests
{
    public class SqlParserTests
    {
        [Fact]
        public void ParseCreateTable_BasicTable_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)";

            // Act
            var (tableName, columns) = SqlParser.ParseCreateTable(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.Equal(3, columns.Count);
            
            Assert.Equal("id", columns[0].Name);
            Assert.Equal(DataType.Integer, columns[0].DataType);
            Assert.True(columns[0].IsPrimaryKey);
            
            Assert.Equal("name", columns[1].Name);
            Assert.Equal(DataType.Text, columns[1].DataType);
            Assert.False(columns[1].IsNullable); // NOT NULL means IsNullable = false
            
            Assert.Equal("email", columns[2].Name);
            Assert.Equal(DataType.Text, columns[2].DataType);
        }

        [Fact]
        public void ParseCreateTable_WithAutoIncrement_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "CREATE TABLE users (id INTEGER AUTO_INCREMENT PRIMARY KEY, name TEXT NOT NULL, email TEXT)";

            // Act
            var (tableName, columns) = SqlParser.ParseCreateTable(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.Equal(3, columns.Count);
            
            var idColumn = columns[0];
            Assert.Equal("id", idColumn.Name);
            Assert.Equal(DataType.Integer, idColumn.DataType);
            Assert.True(idColumn.IsPrimaryKey);
            Assert.True(idColumn.IsAutoIncrement);
        }

        [Fact]
        public void ParseCreateTable_InvalidSyntax_ShouldThrowException()
        {
            // Arrange
            var invalidSql = "INVALID CREATE TABLE SYNTAX";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlParser.ParseCreateTable(invalidSql));
        }

        [Fact]
        public void ParseInsert_BasicInsert_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com')";

            // Act
            var (tableName, columns, values) = SqlParser.ParseInsert(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.NotNull(columns);
            Assert.Equal(3, columns.Count);
            Assert.Contains("id", columns);
            Assert.Contains("name", columns);
            Assert.Contains("email", columns);
            Assert.Equal(3, values.Count);
            Assert.Equal(1, values[0]); // ParseSingleValue returns int, not string
            Assert.Equal("John Doe", values[1]);
            Assert.Equal("john@example.com", values[2]);
        }

        [Fact]
        public void ParseInsert_WithoutColumnList_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "INSERT INTO users VALUES (1, 'John Doe', 'john@example.com')";

            // Act
            var (tableName, columns, values) = SqlParser.ParseInsert(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.Null(columns); // No column list specified
            Assert.Equal(3, values.Count);
            Assert.Equal(1, values[0]);
            Assert.Equal("John Doe", values[1]);
            Assert.Equal("john@example.com", values[2]);
        }

        [Fact]
        public void ParseSelect_BasicSelect_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "SELECT * FROM users";

            // Act
            var selectQuery = SqlParser.ParseSelect(sql);

            // Assert
            Assert.Single(selectQuery.Columns);
            Assert.Equal("*", selectQuery.Columns[0]);
            Assert.Equal("users", selectQuery.TableName);
            Assert.Null(selectQuery.WhereClause);
        }

        [Fact]
        public void ParseSelect_WithSpecificColumns_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "SELECT id, name, email FROM users";

            // Act
            var selectQuery = SqlParser.ParseSelect(sql);

            // Assert
            Assert.Equal(3, selectQuery.Columns.Count);
            Assert.Contains("id", selectQuery.Columns);
            Assert.Contains("name", selectQuery.Columns);
            Assert.Contains("email", selectQuery.Columns);
            Assert.Equal("users", selectQuery.TableName);
            Assert.Null(selectQuery.WhereClause);
        }

        [Fact]
        public void ParseSelect_WithWhereClause_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "SELECT * FROM users WHERE id = 1";

            // Act
            var selectQuery = SqlParser.ParseSelect(sql);

            // Assert
            Assert.Single(selectQuery.Columns);
            Assert.Equal("*", selectQuery.Columns[0]);
            Assert.Equal("users", selectQuery.TableName);
            Assert.NotNull(selectQuery.WhereClause);
            Assert.Contains("id = 1", selectQuery.WhereClause);
        }

        [Fact]
        public void ParseUpdate_BasicUpdate_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "UPDATE users SET email = 'newemail@example.com' WHERE id = 1";

            // Act
            var (tableName, columnName, value, whereClause) = SqlParser.ParseUpdate(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.Equal("email", columnName);
            Assert.Equal("newemail@example.com", value);
            Assert.NotNull(whereClause);
            Assert.Contains("id = 1", whereClause);
        }

        [Fact]
        public void ParseDelete_BasicDelete_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "DELETE FROM users WHERE id = 1";

            // Act
            var (tableName, whereClause) = SqlParser.ParseDelete(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.NotNull(whereClause);
            Assert.Contains("id = 1", whereClause);
        }

        [Fact]
        public void ParseDelete_WithoutWhere_ShouldParseCorrectly()
        {
            // Arrange
            var sql = "DELETE FROM users";

            // Act
            var (tableName, whereClause) = SqlParser.ParseDelete(sql);

            // Assert
            Assert.Equal("users", tableName);
            Assert.Null(whereClause);
        }

        [Theory]
        [InlineData("CREATE TABLE", "users (id INTEGER PRIMARY KEY)", "users")]
        [InlineData("create table", "products (id INTEGER PRIMARY KEY)", "products")]
        [InlineData("Create Table", "orders (id INTEGER PRIMARY KEY)", "orders")]
        public void ParseCreateTable_CaseInsensitive_ShouldParseCorrectly(string createKeyword, string tableDefinition, string expectedTableName)
        {
            // Arrange
            var sql = $"{createKeyword} {tableDefinition}";

            // Act
            var (tableName, columns) = SqlParser.ParseCreateTable(sql);

            // Assert
            Assert.Equal(expectedTableName, tableName);
            Assert.Single(columns);
        }

        [Theory]
        [InlineData("Integer")]
        [InlineData("Text")]
        [InlineData("Real")]
        public void ParseCreateTable_SupportedDataTypes_ShouldParseCorrectly(string dataType)
        {
            // Arrange
            var sql = $"CREATE TABLE test (col {dataType})";

            // Act
            var (tableName, columns) = SqlParser.ParseCreateTable(sql);

            // Assert
            Assert.Equal("test", tableName);
            Assert.Single(columns);
            Assert.Equal("col", columns[0].Name);
            Assert.True(Enum.TryParse<DataType>(dataType, true, out var expectedDataType));
            Assert.Equal(expectedDataType, columns[0].DataType);
        }

        [Fact]
        public void ParseCreateTable_UnsupportedDataType_ShouldThrowException()
        {
            // Arrange
            var sql = "CREATE TABLE test (col UNSUPPORTED_TYPE)";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlParser.ParseCreateTable(sql));
        }

        [Fact]
        public void ParseInsert_InvalidSyntax_ShouldThrowException()
        {
            // Arrange
            var invalidSql = "INVALID INSERT SYNTAX";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlParser.ParseInsert(invalidSql));
        }

        [Fact]
        public void ParseSelect_InvalidSyntax_ShouldThrowException()
        {
            // Arrange
            var invalidSql = "INVALID SELECT SYNTAX";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlParser.ParseSelect(invalidSql));
        }

        [Fact]
        public void ParseUpdate_InvalidSyntax_ShouldThrowException()
        {
            // Arrange
            var invalidSql = "INVALID UPDATE SYNTAX";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlParser.ParseUpdate(invalidSql));
        }

        [Fact]
        public void ParseDelete_InvalidSyntax_ShouldThrowException()
        {
            // Arrange
            var invalidSql = "INVALID DELETE SYNTAX";

            // Act & Assert
            Assert.Throws<ArgumentException>(() => SqlParser.ParseDelete(invalidSql));
        }

        [Theory]
        [InlineData("id = 1", "id", 1, true)]
        [InlineData("id = 1", "id", 2, false)]
        [InlineData("name = 'test_user'", "name", "test_user", true)]
        [InlineData("name = 'test_user'", "name", "wrong_user", false)]
        [InlineData("name != 'test_user'", "name", "test_user", false)]
        [InlineData("name != 'test_user'", "name", "another_user", true)]
        [InlineData("value > 10", "value", 20, true)]
        [InlineData("value > 10", "value", 10, false)]
        [InlineData("value < 100", "value", 50, true)]
        [InlineData("value < 100", "value", 100, false)]
        [InlineData("value >= 20", "value", 20, true)]
        [InlineData("value >= 20", "value", 19, false)]
        [InlineData("value <= 50", "value", 50, true)]
        [InlineData("value <= 50", "value", 51, false)]
        [InlineData("is_active = NULL", "is_active", null, true)]
        [InlineData("is_active != NULL", "is_active", null, false)]
        [InlineData("LEN(name) = 9", "name", "test_user", true)]
        [InlineData("LEN(name) > 5", "name", "test", false)]
        public void ParseWhereClause_VariousConditions_ShouldEvaluateCorrectly(string whereClause, string columnName, object columnValue, bool expectedResult)
        {
            // Arrange
            var predicate = SqlParser.ParseWhereClause(whereClause);
            var row = new Dictionary<string, object?>
            {
                { columnName, columnValue }
            };

            // Act
            var result = predicate(row);

            // Assert
            Assert.Equal(expectedResult, result);
        }

        [Fact]
        public void TestWhereClauseParsing_WithUnderscoreInValue()
        {
            var whereClause = "some_column = 'some_value'";
            var func = SqlParser.ParseWhereClause(whereClause);
            var row = new Dictionary<string, object?> { { "some_column", "some_value" } };
            Assert.True(func(row));
        }

        [Fact]
        public void ParseWhereClause_ManualTest_ShouldWork()
        {
            // Test simple string comparison
            var whereClause1 = "name = 'John'";
            var predicate1 = SqlParser.ParseWhereClause(whereClause1);
            var row1 = new Dictionary<string, object?> { { "name", "John" } };
            var row2 = new Dictionary<string, object?> { { "name", "Jane" } };
            
            Assert.True(predicate1(row1)); // John should match
            Assert.False(predicate1(row2)); // Jane should not match

            // Test LEN function
            var whereClause2 = "LEN(name) = 4";
            var predicate2 = SqlParser.ParseWhereClause(whereClause2);
            var row3 = new Dictionary<string, object?> { { "name", "John" } }; // 4 chars
            var row4 = new Dictionary<string, object?> { { "name", "Alexander" } }; // 9 chars
            
            Assert.True(predicate2(row3)); // 'John' (4 chars) should match
            Assert.False(predicate2(row4)); // 'Alexander' (9 chars) should not match

            // Test numeric comparison
            var whereClause3 = "age > 25";
            var predicate3 = SqlParser.ParseWhereClause(whereClause3);
            var row5 = new Dictionary<string, object?> { { "age", 30 } };
            var row6 = new Dictionary<string, object?> { { "age", 20 } };
            
            Assert.True(predicate3(row5)); // age=30 should match
            Assert.False(predicate3(row6)); // age=20 should not match
        }
    }
}
