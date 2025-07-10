using System;
using BasicSQL.UI;
using BasicSQL.Core;

namespace BasicSQL
{
    /// <summary>
    /// Main program entry point
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            // Start TCP server for remote SQL access (default port 4162)
            int port = 4162;
            var portEnv = Environment.GetEnvironmentVariable("BASICSQL_PORT");
            if (!string.IsNullOrEmpty(portEnv) && int.TryParse(portEnv, out int envPort))
                port = envPort;

            // Always load from the persistent data directory
            var engine = new BinarySqlEngine("binary_data");
            var authManager = new AuthenticationManager(engine);

            var tcpServer = new Core.SqlTcpServer(port, sql =>
            {
                try
                {
                    var result = engine.Execute(sql);
                    if (!result.Success)
                        return $"ERROR: {result.ErrorMessage}";
                    if (result.IsQueryResult)
                    {
                        var sb = new System.Text.StringBuilder();
                        sb.AppendLine(string.Join("\t", result.Columns));
                        foreach (var row in result.Rows)
                            sb.AppendLine(string.Join("\t", result.Columns.Select(c => row.GetValueOrDefault(c)?.ToString() ?? "NULL")));
                        return sb.ToString();
                    }
                    if (result.IsTableListResult)
                        return $"Tables: {string.Join(", ", result.Tables)}";
                    return result.Message;
                }
                catch (Exception ex)
                {
                    return $"ERROR: {ex.Message}";
                }
            }, authManager);
            
            // Check if we're running in batch mode that doesn't need TCP server
            bool skipTcpServer = args.Length > 0 && 
                (args[0].ToLower() == "--benchmark" || args[0].ToLower() == "--benchmark-update-delete" || args[0].ToLower() == "-b" ||
                 args[0].ToLower() == "--scalability" || args[0].ToLower() == "-s" ||
                 args[0].ToLower() == "--quick-scalability" || args[0].ToLower() == "-qs" ||
                 args[0].ToLower() == "--performance" || args[0].ToLower() == "-p" ||
                 args[0].ToLower() == "--demo" || args[0].ToLower() == "-d" ||
                 args[0].ToLower() == "--help" || args[0].ToLower() == "-h" ||
                 args[0].ToLower() == "--file" || args[0].ToLower() == "-f");
            
            if (!skipTcpServer)
            {
                tcpServer.Start();
                Console.WriteLine($"[BasicSQL] TCP server started on port {port}");
            }

            if (args.Length > 0)
            {
                // Run in batch mode with provided arguments
                RunBatchMode(args, authManager);
            }
            else
            {
                // Run interactive CLI
                RunInteractiveMode();
            }
        }

        /// <summary>
        /// Runs the program in interactive CLI mode
        /// </summary>
        private static void RunInteractiveMode()
        {
            try
            {
                var cli = new SqlCli();
                cli.Run();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Fatal error: {ex.Message}");
                Environment.Exit(1);
            }
        }

        /// <summary>
        /// Runs the program in batch mode with command line arguments
        /// </summary>
        private static void RunBatchMode(string[] args, AuthenticationManager authManager)
        {
            var engine = authManager.GetEngine(); // Use the same engine as the auth manager
            
            // Handle command line options
            for (int i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                
                switch (arg.ToLower())
                {
                    case "--create-user":
                        if (i + 2 < args.Length)
                        {
                            var username = args[i + 1];
                            var password = args[i + 2];
                            var role = "user"; // default role
                            if (i + 3 < args.Length && !args[i + 3].StartsWith("--"))
                            {
                                role = args[i + 3];
                                i++; // consume role argument
                            }

                            if (authManager.CreateUser(username, password, role))
                            {
                                Console.WriteLine($"User '{username}' with role '{role}' created successfully.");
                            }
                            else
                            {
                                Console.WriteLine($"Error: Could not create user '{username}'.");
                            }
                            i += 2; // Skip username and password args
                        }
                        else
                        {
                            Console.WriteLine("Usage: --create-user <username> <password> [role]");
                        }
                        break;

                    case "--demo":
                    case "-d":
                        RunDemo(engine);
                        break;
                        
                    case "--performance":
                    case "-p":
                        RunBinaryPerformanceTest();
                        break;
                        
                    case "--scalability":
                    case "-s":
                        RunScalabilityTest();
                        break;
                        
                    case "--quick-scalability":
                    case "-qs":
                        RunQuickScalabilityTest();
                        break;
                        
                    case "--file":
                    case "-f":
                        if (i + 1 < args.Length)
                        {
                            RunSqlFile(engine, args[i + 1]);
                            i++; // Skip the filename argument
                        }
                        else
                        {
                            Console.WriteLine("❌ Error: --file requires a filename argument");
                            Environment.Exit(1);
                        }
                        break;
                        
                    case "--help":
                    case "-h":
                        PrintUsage();
                        return;
                        
                    case "--benchmark":
                    case "--benchmark-update-delete":
                    case "-b":
                        RunUpdateDeleteBenchmark();
                        break;
                        
                    default:
                        Console.WriteLine($"❌ Unknown argument: {arg}");
                        PrintUsage();
                        Environment.Exit(1);
                        break;
                }
            }
        }

        /// <summary>
        /// Runs a demonstration of the binary SQL engine
        /// </summary>
        private static void RunDemo(BinarySqlEngine engine)
        {
            Console.WriteLine("🚀 Running SQL Engine Demo...\n");

            var demoCommands = new[]
            {
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, email TEXT)",
                "INSERT INTO users VALUES (1, 'John Doe', 30, 'john@example.com')",
                "INSERT INTO users VALUES (2, 'Jane Smith', 25, 'jane@example.com')",
                "INSERT INTO users VALUES (3, 'Bob Johnson', 35, 'bob@example.com')",
                "INSERT INTO users VALUES (4, 'Alice Brown', 28, 'alice@example.com')",
                "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL, category TEXT)",
                "INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')",
                "INSERT INTO products VALUES (2, 'Coffee Mug', 12.50, 'Home')",
                "INSERT INTO products VALUES (3, 'Smartphone', 699.99, 'Electronics')",
                "SHOW TABLES",
                "SELECT * FROM users",
                "SELECT name, email FROM users WHERE age > 25",
                "SELECT * FROM users ORDER BY age DESC",
                "SELECT * FROM products WHERE category = 'Electronics'",
                "UPDATE users SET age = 31 WHERE name = 'John Doe'",
                "SELECT * FROM users WHERE name = 'John Doe'",
                "DELETE FROM users WHERE age < 27",
                "SELECT * FROM users"
            };

            foreach (var sql in demoCommands)
            {
                Console.WriteLine($"SQL> {sql}");
                var result = engine.Execute(sql);
                
                if (!result.Success)
                {
                    Console.WriteLine($"❌ ERROR: {result.ErrorMessage}");
                }
                else if (result.IsQueryResult)
                {
                    DisplaySimpleQueryResult(result);
                }
                else if (result.IsTableListResult)
                {
                    Console.WriteLine($"📋 Tables: {string.Join(", ", result.Tables)}");
                }
                else
                {
                    Console.WriteLine($"✅ {result.Message}");
                }

                Console.WriteLine(new string('-', 50));
            }

            Console.WriteLine("\n🎉 Demo completed!");
        }

        /// <summary>
        /// Runs SQL commands from a file using binary engine
        /// </summary>
        private static void RunSqlFile(BinarySqlEngine engine, string filename)
        {
            try
            {
                if (!System.IO.File.Exists(filename))
                {
                    Console.WriteLine($"❌ Error: File '{filename}' not found");
                    Environment.Exit(1);
                }

                var lines = System.IO.File.ReadAllLines(filename);
                var currentCommand = "";

                Console.WriteLine($"📂 Executing SQL from file: {filename}\n");

                foreach (var line in lines)
                {
                    var trimmedLine = line.Trim();
                    
                    // Skip empty lines and comments
                    if (string.IsNullOrEmpty(trimmedLine) || trimmedLine.StartsWith("--"))
                        continue;

                    currentCommand += " " + trimmedLine;

                    // Execute command when we hit a semicolon
                    if (trimmedLine.EndsWith(";"))
                    {
                        var sql = currentCommand.Trim().TrimEnd(';');
                        if (!string.IsNullOrEmpty(sql))
                        {
                            Console.WriteLine($"SQL> {sql}");
                            var result = engine.Execute(sql);
                            
                            if (!result.Success)
                            {
                                Console.WriteLine($"❌ ERROR: {result.ErrorMessage}");
                            }
                            else
                            {
                                Console.WriteLine($"✅ {result.Message}");
                            }
                            Console.WriteLine();
                        }
                        currentCommand = "";
                    }
                }

                // Execute any remaining command
                if (!string.IsNullOrWhiteSpace(currentCommand))
                {
                    var sql = currentCommand.Trim();
                    Console.WriteLine($"SQL> {sql}");
                    var result = engine.Execute(sql);
                    
                    if (!result.Success)
                    {
                        Console.WriteLine($"❌ ERROR: {result.ErrorMessage}");
                    }
                    else
                    {
                        Console.WriteLine($"✅ {result.Message}");
                    }
                }

                Console.WriteLine("📄 File execution completed!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error executing file: {ex.Message}");
                Environment.Exit(1);
            }
        }

        /// <summary>
        /// Displays query results in a simple format for batch mode
        /// </summary>
        private static void DisplaySimpleQueryResult(BasicSQL.Models.SqlResult result)
        {
            if (result.Rows.Count == 0)
            {
                Console.WriteLine("📋 No rows returned");
                return;
            }

            Console.WriteLine($"📊 Columns: {string.Join(", ", result.Columns)}");
            
            for (int i = 0; i < result.Rows.Count; i++)
            {
                var row = result.Rows[i];
                var values = result.Columns.Select(col => 
                    row.GetValueOrDefault(col)?.ToString() ?? "NULL");
                Console.WriteLine($"Row {i + 1}: {string.Join(" | ", values)}");
            }
            
            Console.WriteLine($"({result.Rows.Count} rows)");
        }

        /// <summary>
        /// Prints usage information
        /// </summary>
        private static void PrintUsage()
        {
            Console.WriteLine("BasicSQL Engine - Usage:");
            Console.WriteLine("  BasicSQL                       Run in interactive mode");
            Console.WriteLine("  BasicSQL --demo                Run demonstration");
            Console.WriteLine("  BasicSQL --performance         Run binary storage performance test");
            Console.WriteLine("  BasicSQL --benchmark           Run UPDATE/DELETE performance benchmark");
            Console.WriteLine("  BasicSQL --scalability         Run scalability test (1M records)");
            Console.WriteLine("  BasicSQL --quick-scalability   Run quick scalability test (100K records)");
            Console.WriteLine("  BasicSQL --create-user <user> <pass> Create a new user");
            Console.WriteLine("  BasicSQL --file <filename>     Execute SQL file");
            Console.WriteLine("  BasicSQL --help                Show this help");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  BasicSQL --demo");
            Console.WriteLine("  BasicSQL --benchmark");
            Console.WriteLine("  BasicSQL --file examples.sql");
        }

        /// <summary>
        /// Runs binary storage performance test
        /// </summary>
        private static void RunBinaryPerformanceTest()
        {
            try
            {
                // var test = new Tests.ScalabilityTest();
                // test.RunBinaryPerformanceTest(100_000); // 100K records for performance demo
                Console.WriteLine("⚠️  Binary performance test not available - ScalabilityTest class missing");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Binary performance test failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Runs scalability demonstration
        /// </summary>
        private static void RunScalabilityTest()
        {
            try
            {
                // var test = new Tests.ScalabilityTest();
                // test.DemonstrateHugeDatasetHandling();
                Console.WriteLine("⚠️  Scalability test not available - ScalabilityTest class missing");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Scalability test failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Runs quick scalability demonstration with smaller dataset
        /// </summary>
        private static void RunQuickScalabilityTest()
        {
            try
            {
                // var test = new Tests.ScalabilityTest();
                // test.RunQuickScalabilityDemo();
                Console.WriteLine("⚠️  Quick scalability test not available - ScalabilityTest class missing");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Quick scalability test failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Runs UPDATE and DELETE performance benchmarks
        /// </summary>
        private static void RunUpdateDeleteBenchmark()
        {
            try
            {
                Console.WriteLine("🚀 Starting UPDATE/DELETE Performance Benchmark...");
                Console.WriteLine();
                
                var benchmark = new Benchmarks.UpdateDeleteBenchmark();
                benchmark.RunAllBenchmarks();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Benchmark error: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }
    }
}
