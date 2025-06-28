using System;
using System.Diagnostics;
using System.IO;
using BasicSQL.Core;

namespace BasicSQL.Benchmarks
{
    /// <summary>
    /// Standalone performance benchmark for UPDATE and DELETE operations
    /// </summary>
    public class UpdateDeleteBenchmark
    {
        private readonly BinarySqlEngine _engine;
        private readonly string _benchmarkDataDirectory;

        public UpdateDeleteBenchmark()
        {
            _benchmarkDataDirectory = Path.Combine(Path.GetTempPath(), "benchmark_sql_" + Guid.NewGuid().ToString("N"));
            _engine = new BinarySqlEngine(_benchmarkDataDirectory);
        }

        public static void RunBenchmark()
        {
            Console.WriteLine("=================================================================");
            Console.WriteLine("🚀 Binary SQL Engine - UPDATE/DELETE Performance Benchmark 🚀");
            Console.WriteLine("=================================================================");
            Console.WriteLine();

            var benchmark = new UpdateDeleteBenchmark();
            try
            {
                benchmark.RunAllBenchmarks();
            }
            finally
            {
                benchmark.Cleanup();
            }

            Console.WriteLine();
            Console.WriteLine("=================================================================");
            Console.WriteLine("✅ Benchmark completed successfully!");
            Console.WriteLine("=================================================================");
        }

        public void RunAllBenchmarks()
        {
            RunUpdateBenchmarks();
            Console.WriteLine();
            RunDeleteBenchmarks();
            Console.WriteLine();
            RunComparativeBenchmarks();
        }

        private void RunUpdateBenchmarks()
        {
            Console.WriteLine("📊 UPDATE Operation Benchmarks");
            Console.WriteLine("===============================");

            // Small dataset benchmark
            BenchmarkUpdate("Small Dataset (1K rows)", 1000, 100);
            
            // Medium dataset benchmark  
            BenchmarkUpdate("Medium Dataset (10K rows)", 10000, 500);
            
            // Large dataset benchmark
            BenchmarkUpdate("Large Dataset (50K rows)", 50000, 1000);
            
            // Multi-column update benchmark
            BenchmarkMultiColumnUpdate("Multi-Column Update (5K rows)", 5000, 1000);
        }

        private void RunDeleteBenchmarks()
        {
            Console.WriteLine("📊 DELETE Operation Benchmarks");
            Console.WriteLine("===============================");

            // Small dataset benchmark
            BenchmarkDelete("Small Dataset (1K rows)", 1000, 100);
            
            // Medium dataset benchmark
            BenchmarkDelete("Medium Dataset (10K rows)", 10000, 500);
            
            // Large dataset benchmark
            BenchmarkDelete("Large Dataset (50K rows)", 50000, 1000);
        }

        private void RunComparativeBenchmarks()
        {
            Console.WriteLine("📊 Comparative Benchmarks");
            Console.WriteLine("==========================");

            // Batch vs Individual operations
            BenchmarkBatchVsIndividual();
            
            // No-match operations
            BenchmarkNoMatchOperations();
        }

        private void BenchmarkUpdate(string testName, int totalRows, int updateRows)
        {
            var tableName = $"update_test_{Guid.NewGuid():N}";
            
            // Setup
            var setupTime = SetupTestTable(tableName, totalRows);
            
            // Benchmark UPDATE
            var stopwatch = Stopwatch.StartNew();
            var result = _engine.Execute($"UPDATE {tableName} SET age = 99 WHERE id <= {updateRows}");
            stopwatch.Stop();
            
            // Results
            var updateTime = stopwatch.ElapsedMilliseconds;
            var rowsPerSecond = updateRows * 1000.0 / Math.Max(updateTime, 1);
            var percentage = (double)updateRows / totalRows * 100;
            
            Console.WriteLine($"  {testName}:");
            Console.WriteLine($"    Setup: {setupTime}ms");
            Console.WriteLine($"    Update {updateRows:N0} rows ({percentage:F1}%): {updateTime}ms");
            Console.WriteLine($"    Performance: {rowsPerSecond:F0} rows/second");
            Console.WriteLine($"    Success: {result.Success}, Affected: {result.RowsAffected}");
            Console.WriteLine();
        }

        private void BenchmarkDelete(string testName, int totalRows, int deleteRows)
        {
            var tableName = $"delete_test_{Guid.NewGuid():N}";
            
            // Setup
            var setupTime = SetupTestTable(tableName, totalRows);
            
            // Benchmark DELETE
            var stopwatch = Stopwatch.StartNew();
            var result = _engine.Execute($"DELETE FROM {tableName} WHERE id <= {deleteRows}");
            stopwatch.Stop();
            
            // Results
            var deleteTime = stopwatch.ElapsedMilliseconds;
            var rowsPerSecond = deleteRows * 1000.0 / Math.Max(deleteTime, 1);
            var percentage = (double)deleteRows / totalRows * 100;
            
            Console.WriteLine($"  {testName}:");
            Console.WriteLine($"    Setup: {setupTime}ms");
            Console.WriteLine($"    Delete {deleteRows:N0} rows ({percentage:F1}%): {deleteTime}ms");
            Console.WriteLine($"    Performance: {rowsPerSecond:F0} rows/second");
            Console.WriteLine($"    Success: {result.Success}, Affected: {result.RowsAffected}");
            Console.WriteLine();
        }

        private void BenchmarkMultiColumnUpdate(string testName, int totalRows, int updateRows)
        {
            var tableName = $"multi_update_test_{Guid.NewGuid():N}";
            
            // Setup
            var setupTime = SetupTestTable(tableName, totalRows);
            
            // Benchmark multi-column UPDATE
            var stopwatch = Stopwatch.StartNew();
            var result = _engine.Execute($"UPDATE {tableName} SET age = 45, name = 'Updated User' WHERE id <= {updateRows}");
            stopwatch.Stop();
            
            // Results
            var updateTime = stopwatch.ElapsedMilliseconds;
            var rowsPerSecond = updateRows * 1000.0 / Math.Max(updateTime, 1);
            
            Console.WriteLine($"  {testName}:");
            Console.WriteLine($"    Setup: {setupTime}ms");
            Console.WriteLine($"    Multi-column update {updateRows:N0} rows: {updateTime}ms");
            Console.WriteLine($"    Performance: {rowsPerSecond:F0} rows/second");
            Console.WriteLine($"    Success: {result.Success}, Affected: {result.RowsAffected}");
            Console.WriteLine();
        }

        private void BenchmarkBatchVsIndividual()
        {
            var tableName1 = $"batch_test1_{Guid.NewGuid():N}";
            var tableName2 = $"batch_test2_{Guid.NewGuid():N}";
            const int totalRows = 10000;
            
            // Setup two identical tables
            SetupTestTable(tableName1, totalRows);
            SetupTestTable(tableName2, totalRows);
            
            // Test 1: 20 individual updates
            var stopwatch1 = Stopwatch.StartNew();
            for (int i = 1; i <= 20; i++)
            {
                _engine.Execute($"UPDATE {tableName1} SET age = {i + 100} WHERE id = {i * 50}");
            }
            stopwatch1.Stop();
            
            // Test 2: 1 batch update
            var stopwatch2 = Stopwatch.StartNew();
            var result = _engine.Execute($"UPDATE {tableName2} SET age = 200 WHERE id <= 1000");
            stopwatch2.Stop();
            
            // Results
            var individualTime = stopwatch1.ElapsedMilliseconds;
            var batchTime = stopwatch2.ElapsedMilliseconds;
            var ratio = (double)individualTime / Math.Max(batchTime, 1);
            
            Console.WriteLine("  Batch vs Individual Operations:");
            Console.WriteLine($"    20 individual UPDATEs: {individualTime}ms");
            Console.WriteLine($"    1 batch UPDATE (1000 rows): {batchTime}ms");
            Console.WriteLine($"    Batch is {ratio:F1}x faster");
            Console.WriteLine($"    Batch affected: {result.RowsAffected} rows");
            Console.WriteLine();
        }

        private void BenchmarkNoMatchOperations()
        {
            var tableName = $"no_match_test_{Guid.NewGuid():N}";
            const int totalRows = 20000;
            
            // Setup
            var setupTime = SetupTestTable(tableName, totalRows);
            
            // Test UPDATE with no matches
            var stopwatch1 = Stopwatch.StartNew();
            var updateResult = _engine.Execute($"UPDATE {tableName} SET age = 999 WHERE id > 1000000");
            stopwatch1.Stop();
            
            // Test DELETE with no matches
            var stopwatch2 = Stopwatch.StartNew();
            var deleteResult = _engine.Execute($"DELETE FROM {tableName} WHERE id > 1000000");
            stopwatch2.Stop();
            
            Console.WriteLine("  No-Match Operations:");
            Console.WriteLine($"    Setup: {setupTime}ms ({totalRows:N0} rows)");
            Console.WriteLine($"    UPDATE (no matches): {stopwatch1.ElapsedMilliseconds}ms, Affected: {updateResult.RowsAffected}");
            Console.WriteLine($"    DELETE (no matches): {stopwatch2.ElapsedMilliseconds}ms, Affected: {deleteResult.RowsAffected}");
            Console.WriteLine();
        }

        private long SetupTestTable(string tableName, int rowCount)
        {
            var stopwatch = Stopwatch.StartNew();
            
            // Create table
            var createResult = _engine.Execute($"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, email TEXT)");
            if (!createResult.Success)
            {
                throw new Exception($"Failed to create table: {createResult.Message}");
            }
            
            // Insert test data
            for (int i = 1; i <= rowCount; i++)
            {
                var name = $"User{i}";
                var age = 20 + (i % 50);
                var email = $"user{i}@example.com";
                
                var insertResult = _engine.Execute($"INSERT INTO {tableName} VALUES ({i}, '{name}', {age}, '{email}')");
                if (!insertResult.Success)
                {
                    throw new Exception($"Failed to insert row {i}: {insertResult.Message}");
                }
            }
            
            stopwatch.Stop();
            return stopwatch.ElapsedMilliseconds;
        }

        private void Cleanup()
        {
            try
            {
                if (Directory.Exists(_benchmarkDataDirectory))
                {
                    Directory.Delete(_benchmarkDataDirectory, true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"⚠️  Cleanup warning: {ex.Message}");
            }
        }
    }
}
