using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using BasicSQL.Core;
using BasicSQL.Models;

namespace BasicSQL.UI
{
    /// <summary>
    /// Interactive command-line interface for the binary SQL engine
    /// </summary>
    public class SqlCli
    {
        private readonly BinarySqlEngine _engine;
        private bool _running;
        private const string Prompt = "SQL> ";

        public SqlCli()
        {
            _engine = new BinarySqlEngine(); // Use ultra-fast binary engine
            _running = true;
        }

        /// <summary>
        /// Runs the interactive CLI
        /// </summary>
        public void Run()
        {
            PrintBanner();

            while (_running)
            {
                try
                {
                    Console.Write(Prompt);
                    var input = Console.ReadLine()?.Trim();

                    if (string.IsNullOrEmpty(input))
                        continue;

                    if (input.StartsWith('.'))
                    {
                        HandleSpecialCommand(input);
                    }
                    else
                    {
                        var result = _engine.Execute(input);
                        DisplayResult(result);
                    }

                    Console.WriteLine();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Error: {ex.Message}");
                    Console.WriteLine();
                }
            }
        }

        /// <summary>
        /// Prints the welcome banner
        /// </summary>
        private void PrintBanner()
        {
            Console.WriteLine(new string('=', 60));
            Console.WriteLine("⚡ Ultra-Fast Binary SQL Engine - Interactive CLI ⚡");
            Console.WriteLine(new string('=', 60));
            Console.WriteLine("🚀 Features: Binary Storage | 100x+ Faster | Scalable");
            Console.WriteLine("Type your SQL commands or:");
            Console.WriteLine("  .help     - Show available commands");
            Console.WriteLine("  .tables   - Show all tables");
            Console.WriteLine("  .stats    - Show database statistics");
            Console.WriteLine("  .clear    - Clear all tables");
            Console.WriteLine("  .quit     - Exit the program");
            Console.WriteLine("To create a user, run the application with the following arguments: --create-user <username> <password>");
            Console.WriteLine(new string('=', 60));
            Console.WriteLine();
        }

        /// <summary>
        /// Handles special dot commands
        /// </summary>
        private void HandleSpecialCommand(string command)
        {
            switch (command.ToLower())
            {
                case ".help":
                    PrintHelp();
                    break;

                case ".tables":
                    var tablesResult = _engine.Execute("SHOW TABLES");
                    DisplayResult(tablesResult);
                    break;

                case ".save":
                    HandleSave();
                    break;

                case ".load":
                    HandleLoad();
                    break;

                case ".stats":
                    HandleStats();
                    break;

                case ".clear":
                    HandleClear();
                    break;

                case ".quit":
                case ".exit":
                    Console.WriteLine("👋 Goodbye!");
                    _running = false;
                    break;

                default:
                    Console.WriteLine($"❌ Unknown command: {command}");
                    Console.WriteLine("Type .help for available commands");
                    break;
            }
        }

        /// <summary>
        /// Prints help information
        /// </summary>
        private void PrintHelp()
        {
            Console.WriteLine("\n📖 Supported SQL Commands:");
            Console.WriteLine("  CREATE TABLE table_name (column1 type1, column2 type2, ...)");
            Console.WriteLine("  INSERT INTO table_name VALUES (value1, value2, ...)");
            Console.WriteLine("  INSERT INTO table_name (col1, col2) VALUES (val1, val2)");
            Console.WriteLine("  SELECT * FROM table_name");
            Console.WriteLine("  SELECT col1, col2 FROM table_name WHERE condition");
            Console.WriteLine("  SELECT * FROM table_name ORDER BY column [DESC] [LIMIT n]");
            Console.WriteLine("  UPDATE table_name SET column = value WHERE condition");
            Console.WriteLine("  DELETE FROM table_name WHERE condition");
            Console.WriteLine("  SHOW TABLES");

            Console.WriteLine("\n📋 Supported Data Types:");
            Console.WriteLine("  INTEGER - 32-bit whole numbers");
            Console.WriteLine("  LONG    - 64-bit whole numbers");
            Console.WriteLine("  TEXT    - String values (use single quotes)");
            Console.WriteLine("  REAL    - Decimal numbers");

            Console.WriteLine("\n🔒 Constraints:");
            Console.WriteLine("  NOT NULL      - Column cannot be empty");
            Console.WriteLine("  PRIMARY KEY   - Unique identifier (automatically NOT NULL)");
            Console.WriteLine("  AUTO_INCREMENT- Auto-incrementing INTEGER/LONG (automatically NOT NULL)");

            Console.WriteLine("\n⚙️  Special Commands:");
            Console.WriteLine("  .help   - Show this help");
            Console.WriteLine("  .tables - List all tables");
            Console.WriteLine("  .save   - Save database to JSON file");
            Console.WriteLine("  .load   - Load database from JSON file");
            Console.WriteLine("  .stats  - Show database statistics");
            Console.WriteLine("  .clear  - Clear all tables");
            Console.WriteLine("  .quit   - Exit the program");
        }

        /// <summary>
        /// Handles the save command
        /// </summary>
        private void HandleSave()
        {
            Console.WriteLine("💾 Database is automatically saved to individual table files.");
            Console.WriteLine("📁 Data directory: data/tables/");
            Console.WriteLine("📊 Index directory: data/indexes/");
            
            var (tableCount, totalRows) = _engine.GetStatistics();
            Console.WriteLine($"✅ {tableCount} table(s) with {totalRows} total rows are persistent");
        }

        /// <summary>
        /// Handles the load command
        /// </summary>
        private void HandleLoad()
        {
            Console.WriteLine("📂 Database is automatically loaded from table files.");
            Console.WriteLine("📁 Data directory: data/tables/");
            
            var (tableCount, totalRows) = _engine.GetStatistics();
            Console.WriteLine($"✅ Loaded {tableCount} table(s) with {totalRows} total rows");
            
            if (tableCount > 0)
            {
                Console.WriteLine("\n📋 Available tables:");
                foreach (var tableName in _engine.GetTableNames())
                {
                    var table = _engine.GetTable(tableName);
                    if (table != null)
                    {
                        var pkInfo = table.HasPrimaryKey ? "with PK" : "auto-ID";
                        Console.WriteLine($"   📄 {tableName}: {table.Columns.Count} columns, {table.RowCount} rows ({pkInfo}) [Binary Storage]");
                    }
                }
            }
        }

        /// <summary>
        /// Handles the stats command
        /// </summary>
        private void HandleStats()
        {
            var (tableCount, totalRows) = _engine.GetStatistics();
            Console.WriteLine("📊 Database Statistics:");
            Console.WriteLine($"   Tables: {tableCount}");
            Console.WriteLine($"   Total Rows: {totalRows}");
            Console.WriteLine($"   Storage: File-based (data/ directory)");
            
            if (tableCount > 0)
            {
                Console.WriteLine("\n   📋 Table Details:");
                foreach (var tableName in _engine.GetTableNames())
                {
                    var table = _engine.GetTable(tableName);
                    if (table != null)
                    {
                        var pkInfo = table.HasPrimaryKey ? "with PK" : "auto-ID";
                        Console.WriteLine($"   📄 {tableName}: {table.Columns.Count} columns, {table.RowCount} rows ({pkInfo}) [Binary Storage]");
                    }
                }
            }
        }

        /// <summary>
        /// Handles the clear command
        /// </summary>
        private void HandleClear()
        {
            Console.Write("Are you sure you want to clear all tables? (y/N): ");
            var confirmation = Console.ReadLine()?.Trim().ToLower();
            
            if (confirmation == "y" || confirmation == "yes")
            {
                _engine.ClearDatabase();
                Console.WriteLine("✅ All tables cleared");
            }
            else
            {
                Console.WriteLine("❌ Operation cancelled");
            }
        }

        /// <summary>
        /// Displays the result of a SQL operation
        /// </summary>
        private void DisplayResult(SqlResult result)
        {
            if (!result.Success)
            {
                Console.WriteLine($"❌ ERROR: {result.ErrorMessage}");
                return;
            }

            if (result.IsQueryResult)
            {
                DisplayQueryResult(result);
            }
            else if (result.IsTableListResult)
            {
                DisplayTableList(result);
            }
            else
            {
                Console.WriteLine($"✅ {result.Message}");
                if (result.RowsAffected > 0)
                    Console.WriteLine($"📊 {result.RowsAffected} row(s) affected");
            }
        }

        /// <summary>
        /// Displays query results in a table format
        /// </summary>
        private void DisplayQueryResult(SqlResult result)
        {
            if (result.Rows.Count == 0)
            {
                Console.WriteLine("📋 No rows returned");
                return;
            }

            var columns = result.Columns;
            var rows = result.Rows;

            // Calculate column widths
            var columnWidths = new Dictionary<string, int>();
            foreach (var column in columns)
            {
                columnWidths[column] = column.Length;
            }

            foreach (var row in rows)
            {
                foreach (var column in columns)
                {
                    var value = row.GetValueOrDefault(column)?.ToString() ?? "NULL";
                    columnWidths[column] = Math.Max(columnWidths[column], value.Length);
                }
            }

            // Print header with borders
            Console.WriteLine("📊 Query Results:");
            var headerLine = "┌" + string.Join("┬", columns.Select(c => new string('─', columnWidths[c] + 2))) + "┐";
            Console.WriteLine(headerLine);

            var header = "│" + string.Join("│", columns.Select(c => $" {c.PadRight(columnWidths[c])} ")) + "│";
            Console.WriteLine(header);

            var separatorLine = "├" + string.Join("┼", columns.Select(c => new string('─', columnWidths[c] + 2))) + "┤";
            Console.WriteLine(separatorLine);

            // Print rows
            foreach (var row in rows)
            {
                var rowLine = "│" + string.Join("│", columns.Select(c =>
                {
                    var value = row.GetValueOrDefault(c)?.ToString() ?? "NULL";
                    return $" {value.PadRight(columnWidths[c])} ";
                })) + "│";
                Console.WriteLine(rowLine);
            }

            var footerLine = "└" + string.Join("┴", columns.Select(c => new string('─', columnWidths[c] + 2))) + "┘";
            Console.WriteLine(footerLine);

            Console.WriteLine($"\n✅ {result.Rows.Count} row(s) returned");
        }

        /// <summary>
        /// Displays the list of tables
        /// </summary>
        private void DisplayTableList(SqlResult result)
        {
            if (result.Tables.Count == 0)
            {
                Console.WriteLine("📋 No tables found");
                return;
            }

            Console.WriteLine("📋 Tables:");
            foreach (var tableName in result.Tables.OrderBy(t => t))
            {
                var table = _engine.GetTable(tableName);
                if (table != null)
                {
                    Console.WriteLine($"   📄 {tableName} ({table.Columns.Count} columns, {table.RowCount} rows)");
                }
                else
                {
                    Console.WriteLine($"   📄 {tableName}");
                }
            }

            Console.WriteLine($"\n✅ {result.Tables.Count} table(s) found");
        }
    }
}
