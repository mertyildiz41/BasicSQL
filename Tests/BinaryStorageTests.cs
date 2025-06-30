using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using BasicSQL.Storage;
using BasicSQL.Models;

namespace BasicSQL.Tests
{
    public class BinaryStorageTests : IDisposable
    {
        private readonly HighPerformanceStorageManager _storageManager;
        private readonly string _testDataDirectory;

        public BinaryStorageTests()
        {
            _testDataDirectory = Path.Combine(Path.GetTempPath(), "test_binary_storage_" + Guid.NewGuid().ToString("N"));
            _storageManager = new HighPerformanceStorageManager(_testDataDirectory);
        }

        [Fact]
        public void BinaryStorage_DateTime_ShouldSerializeAndDeserializeCorrectly()
        {
            // Arrange
            var testDateTime = new DateTime(2024, 1, 15, 9, 30, 45, 123);
            var tableName = "datetime_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "test_date", DataType = "DATETIME" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["test_date"] = testDateTime
            };

            // Act
            var rowId = _storageManager.AppendRowBinary(tableName, rowData, metadata);
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();

            // Assert
            Assert.Equal(0, rowId);
            Assert.Single(retrievedRows);
            
            var (retrievedRowId, retrievedRow) = retrievedRows.First();
            Assert.Equal(0, retrievedRowId);
            Assert.Equal(testDateTime, retrievedRow["test_date"]);
        }

        [Fact]
        public void BinaryStorage_Decimal_ShouldSerializeAndDeserializeCorrectly()
        {
            // Arrange
            var testDecimal = 123456789.987654321m;
            var tableName = "decimal_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "amount", DataType = "DECIMAL" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["amount"] = testDecimal
            };

            // Act
            var rowId = _storageManager.AppendRowBinary(tableName, rowData, metadata);
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();

            // Assert
            Assert.Equal(0, rowId);
            Assert.Single(retrievedRows);
            
            var (retrievedRowId, retrievedRow) = retrievedRows.First();
            Assert.Equal(0, retrievedRowId);
            Assert.Equal(testDecimal, retrievedRow["amount"]);
        }

        [Fact]
        public void BinaryStorage_NegativeDecimal_ShouldSerializeCorrectly()
        {
            // Arrange
            var testDecimal = -999.123456789m;
            var tableName = "negative_decimal_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "balance", DataType = "DECIMAL" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["balance"] = testDecimal
            };

            // Act
            var rowId = _storageManager.AppendRowBinary(tableName, rowData, metadata);
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();

            // Assert
            Assert.Equal(0, rowId);
            Assert.Single(retrievedRows);
            
            var (retrievedRowId, retrievedRow) = retrievedRows.First();
            Assert.Equal(0, retrievedRowId);
            Assert.Equal(testDecimal, retrievedRow["balance"]);
        }

        [Fact]
        public void BinaryStorage_ZeroDecimal_ShouldSerializeCorrectly()
        {
            // Arrange
            var testDecimal = 0.0m;
            var tableName = "zero_decimal_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "value", DataType = "DECIMAL" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["value"] = testDecimal
            };

            // Act
            var rowId = _storageManager.AppendRowBinary(tableName, rowData, metadata);
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();

            // Assert
            Assert.Equal(0, rowId);
            Assert.Single(retrievedRows);
            
            var (retrievedRowId, retrievedRow) = retrievedRows.First();
            Assert.Equal(0, retrievedRowId);
            Assert.Equal(testDecimal, retrievedRow["value"]);
        }

        [Fact]
        public void BinaryStorage_EdgeCaseDateTime_ShouldSerializeCorrectly()
        {
            // Arrange
            var minDateTime = DateTime.MinValue;
            var maxDateTime = DateTime.MaxValue;
            var tableName = "datetime_edge_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "min_date", DataType = "DATETIME" },
                new ColumnFileData { Name = "max_date", DataType = "DATETIME" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["min_date"] = minDateTime,
                ["max_date"] = maxDateTime
            };

            // Act
            var rowId = _storageManager.AppendRowBinary(tableName, rowData, metadata);
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();

            // Assert
            Assert.Equal(0, rowId);
            Assert.Single(retrievedRows);
            
            var (retrievedRowId, retrievedRow) = retrievedRows.First();
            Assert.Equal(0, retrievedRowId);
            Assert.Equal(minDateTime, retrievedRow["min_date"]);
            Assert.Equal(maxDateTime, retrievedRow["max_date"]);
        }

        [Fact]
        public void BinaryStorage_EdgeCaseDecimal_ShouldSerializeCorrectly()
        {
            // Arrange
            var minDecimal = decimal.MinValue;
            var maxDecimal = decimal.MaxValue;
            var tableName = "decimal_edge_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "min_value", DataType = "DECIMAL" },
                new ColumnFileData { Name = "max_value", DataType = "DECIMAL" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["min_value"] = minDecimal,
                ["max_value"] = maxDecimal
            };

            // Act
            var rowId = _storageManager.AppendRowBinary(tableName, rowData, metadata);
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();

            // Assert
            Assert.Equal(0, rowId);
            Assert.Single(retrievedRows);
            
            var (retrievedRowId, retrievedRow) = retrievedRows.First();
            Assert.Equal(0, retrievedRowId);
            Assert.Equal(minDecimal, retrievedRow["min_value"]);
            Assert.Equal(maxDecimal, retrievedRow["max_value"]);
        }

        [Fact]
        public void BinaryStorage_ProcessRowsBatch_WithDecimalUpdate_ShouldWorkCorrectly()
        {
            // Arrange
            var tableName = "batch_decimal_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "price", DataType = "DECIMAL" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            // Insert test data
            var originalPrice = 100.50m;
            var newPrice = 150.75m;
            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["price"] = originalPrice
            };

            _storageManager.AppendRowBinary(tableName, rowData, metadata);

            // Act - Update using batch processing
            var updatedCount = _storageManager.ProcessRowsBatchBinary(
                tableName, 
                metadata,
                row => (int)row["id"] == 1,
                row => new Dictionary<string, object?> { ["id"] = row["id"], ["price"] = newPrice }
            );

            // Assert
            Assert.Equal(1, updatedCount);
            
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();
            Assert.Single(retrievedRows);
            
            var (_, retrievedRow) = retrievedRows.First();
            Assert.Equal(newPrice, retrievedRow["price"]);
        }

        [Fact]
        public void BinaryStorage_ProcessRowsBatch_WithDateTimeUpdate_ShouldWorkCorrectly()
        {
            // Arrange
            var tableName = "batch_datetime_test";
            var columns = new List<ColumnFileData>
            {
                new ColumnFileData { Name = "id", DataType = "INTEGER" },
                new ColumnFileData { Name = "event_date", DataType = "DATETIME" }
            };
            var metadata = new ScalableTableMetadata
            {
                TableName = tableName,
                Columns = columns,
                TotalRows = 0,
                Created = DateTime.UtcNow,
                LastModified = DateTime.UtcNow
            };

            // Insert test data
            var originalDate = new DateTime(2024, 1, 15, 9, 0, 0);
            var newDate = new DateTime(2024, 2, 15, 10, 30, 0);
            var rowData = new Dictionary<string, object?>
            {
                ["id"] = 1,
                ["event_date"] = originalDate
            };

            _storageManager.AppendRowBinary(tableName, rowData, metadata);

            // Act - Update using batch processing
            var updatedCount = _storageManager.ProcessRowsBatchBinary(
                tableName, 
                metadata,
                row => (int)row["id"] == 1,
                row => new Dictionary<string, object?> { ["id"] = row["id"], ["event_date"] = newDate }
            );

            // Assert
            Assert.Equal(1, updatedCount);
            
            var retrievedRows = _storageManager.ReadRowsBinary(tableName, metadata, 0, 1).ToList();
            Assert.Single(retrievedRows);
            
            var (_, retrievedRow) = retrievedRows.First();
            Assert.Equal(newDate, retrievedRow["event_date"]);
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
