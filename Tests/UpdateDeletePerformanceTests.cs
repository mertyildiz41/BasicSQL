using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Xunit;
using Xunit.Abstractions;
using BasicSQL.Core;

namespace BasicSQL.Tests
{
    /// <summary>
    /// Performance tests for UPDATE and DELETE operations on binary storage
    /// </summary>
    public class UpdateDeletePerformanceTests : IDisposable
    {
        private readonly BinarySqlEngine _engine;
        private readonly string _testDataDirectory;
        private readonly ITestOutputHelper _output;

        public UpdateDeletePerformanceTests(ITestOutputHelper output)
        {
            _output = output;
            _testDataDirectory = Path.Combine(Path.GetTempPath(), "perf_test_sql_" + Guid.NewGuid().ToString("N"));
            _engine = new BinarySqlEngine(_testDataDirectory);
        }

        [Fact]
        public void UpdatePerformance_SmallDataset_ShouldBeEfficient()
        {
            // Arrange: Create table with 1,000 rows
            const int rowCount = 1000;
            SetupTestTable("small_update_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Update 10% of records
            stopwatch.Start();
            var result = _engine.Execute("UPDATE small_update_test SET age = 99 WHERE id <= 100");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(100, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"UPDATE 100 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 500ms for small dataset
            Assert.True(elapsedMs < 500, $"UPDATE took {elapsedMs}ms, expected < 500ms");
        }

        [Fact]
        public void UpdatePerformance_MediumDataset_ShouldBeReasonablyFast()
        {
            // Arrange: Create table with 10,000 rows
            const int rowCount = 10000;
            SetupTestTable("medium_update_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Update 5% of records
            stopwatch.Start();
            var result = _engine.Execute("UPDATE medium_update_test SET age = 88 WHERE id <= 500");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(500, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"UPDATE 500 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 2 seconds for medium dataset
            Assert.True(elapsedMs < 2000, $"UPDATE took {elapsedMs}ms, expected < 2000ms");
        }

        [Fact]
        public void UpdatePerformance_LargeDataset_ShouldHandleReasonably()
        {
            // Arrange: Create table with 50,000 rows
            const int rowCount = 50000;
            SetupTestTable("large_update_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Update 2% of records
            stopwatch.Start();
            var result = _engine.Execute("UPDATE large_update_test SET age = 77 WHERE id <= 1000");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(1000, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"UPDATE 1000 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 10 seconds for large dataset
            Assert.True(elapsedMs < 10000, $"UPDATE took {elapsedMs}ms, expected < 10000ms");
        }

        [Fact]
        public void DeletePerformance_SmallDataset_ShouldBeEfficient()
        {
            // Arrange: Create table with 1,000 rows
            const int rowCount = 1000;
            SetupTestTable("small_delete_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Delete 10% of records
            stopwatch.Start();
            var result = _engine.Execute("DELETE FROM small_delete_test WHERE id <= 100");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(100, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"DELETE 100 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 500ms for small dataset
            Assert.True(elapsedMs < 500, $"DELETE took {elapsedMs}ms, expected < 500ms");
        }

        [Fact]
        public void DeletePerformance_MediumDataset_ShouldBeReasonablyFast()
        {
            // Arrange: Create table with 10,000 rows
            const int rowCount = 10000;
            SetupTestTable("medium_delete_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Delete 5% of records
            stopwatch.Start();
            var result = _engine.Execute("DELETE FROM medium_delete_test WHERE id <= 500");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(500, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"DELETE 500 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 2 seconds for medium dataset
            Assert.True(elapsedMs < 2000, $"DELETE took {elapsedMs}ms, expected < 2000ms");
        }

        [Fact]
        public void DeletePerformance_LargeDataset_ShouldHandleReasonably()
        {
            // Arrange: Create table with 50,000 rows
            const int rowCount = 50000;
            SetupTestTable("large_delete_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Delete 2% of records
            stopwatch.Start();
            var result = _engine.Execute("DELETE FROM large_delete_test WHERE id <= 1000");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(1000, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"DELETE 1000 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 10 seconds for large dataset
            Assert.True(elapsedMs < 10000, $"DELETE took {elapsedMs}ms, expected < 10000ms");
        }

        [Fact]
        public void UpdatePerformance_MultipleColumns_ShouldBeEfficient()
        {
            // Arrange: Create table with 5,000 rows
            const int rowCount = 5000;
            SetupTestTable("multi_column_update_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Update multiple columns for 20% of records
            stopwatch.Start();
            var result = _engine.Execute("UPDATE multi_column_update_test SET age = 45, name = 'Updated User' WHERE id <= 1000");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(1000, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"UPDATE multiple columns for 1000 out of {rowCount} rows: {elapsedMs}ms");
            
            // Should complete in under 3 seconds for multi-column update
            Assert.True(elapsedMs < 3000, $"Multi-column UPDATE took {elapsedMs}ms, expected < 3000ms");
        }

        [Fact]
        public void DeletePerformance_NoMatches_ShouldBeFast()
        {
            // Arrange: Create table with 10,000 rows
            const int rowCount = 10000;
            SetupTestTable("no_match_delete_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Try to delete records that don't exist
            stopwatch.Start();
            var result = _engine.Execute("DELETE FROM no_match_delete_test WHERE id > 100000");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(0, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"DELETE with no matches from {rowCount} rows: {elapsedMs}ms");
            
            // Should complete very quickly when no matches
            Assert.True(elapsedMs < 1000, $"No-match DELETE took {elapsedMs}ms, expected < 1000ms");
        }

        [Fact]
        public void UpdatePerformance_NoMatches_ShouldBeFast()
        {
            // Arrange: Create table with 10,000 rows
            const int rowCount = 10000;
            SetupTestTable("no_match_update_test", rowCount);

            var stopwatch = new Stopwatch();
            
            // Act: Try to update records that don't exist
            stopwatch.Start();
            var result = _engine.Execute("UPDATE no_match_update_test SET age = 999 WHERE id > 100000");
            stopwatch.Stop();

            // Assert
            Assert.True(result.Success);
            Assert.Equal(0, result.RowsAffected);
            
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            _output.WriteLine($"UPDATE with no matches from {rowCount} rows: {elapsedMs}ms");
            
            // Should complete very quickly when no matches
            Assert.True(elapsedMs < 1000, $"No-match UPDATE took {elapsedMs}ms, expected < 1000ms");
        }

        [Fact]
        public void UpdateDelete_BatchOperations_PerformanceComparison()
        {
            // Arrange: Create table with 20,000 rows
            const int rowCount = 20000;
            SetupTestTable("batch_comparison_test", rowCount);

            // Test 1: Multiple small updates
            var stopwatch1 = new Stopwatch();
            stopwatch1.Start();
            for (int i = 1; i <= 100; i += 10)
            {
                _engine.Execute($"UPDATE batch_comparison_test SET age = {i + 50} WHERE id = {i}");
            }
            stopwatch1.Stop();

            // Test 2: One large update
            SetupTestTable("batch_comparison_test2", rowCount);
            var stopwatch2 = new Stopwatch();
            stopwatch2.Start();
            _engine.Execute("UPDATE batch_comparison_test2 SET age = 150 WHERE id <= 100");
            stopwatch2.Stop();

            // Assert and report
            var multipleUpdatesMs = stopwatch1.ElapsedMilliseconds;
            var singleUpdateMs = stopwatch2.ElapsedMilliseconds;
            
            _output.WriteLine($"10 individual UPDATE operations: {multipleUpdatesMs}ms");
            _output.WriteLine($"1 batch UPDATE operation (100 rows): {singleUpdateMs}ms");
            _output.WriteLine($"Performance ratio: {(double)multipleUpdatesMs / singleUpdateMs:F2}x");

            // Batch operations should be significantly faster
            Assert.True(singleUpdateMs < multipleUpdatesMs, 
                $"Batch update ({singleUpdateMs}ms) should be faster than multiple individual updates ({multipleUpdatesMs}ms)");
        }

        private void SetupTestTable(string tableName, int rowCount)
        {
            // Create table
            var createResult = _engine.Execute($"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)");
            Assert.True(createResult.Success, $"Failed to create table: {createResult.Message}");

            // Insert test data in batches for better performance
            const int batchSize = 1000;
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"User{i}";
                var age = 20 + (i % 50); // Ages between 20-69
                var email = $"user{i}@example.com";
                
                var insertResult = _engine.Execute($"INSERT INTO {tableName} VALUES ({i}, '{name}', {age}, '{email}')");
                Assert.True(insertResult.Success, $"Failed to insert row {i}: {insertResult.Message}");

                // Log progress for large datasets
                if (i % batchSize == 0)
                {
                    _output.WriteLine($"Inserted {i}/{rowCount} rows...");
                }
            }

            stopwatch.Stop();
            _output.WriteLine($"Setup: Inserted {rowCount} rows in {stopwatch.ElapsedMilliseconds}ms");
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDataDirectory))
            {
                try
                {
                    Directory.Delete(_testDataDirectory, true);
                }
                catch
                {
                    // Ignore cleanup errors in tests
                }
            }
        }
    }
}
