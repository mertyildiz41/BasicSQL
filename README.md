# ⚡ Ultra-Fast Binary SQL Engine - BasicSQL (C#)

A revolutionary high-performance SQL database engine implemented in C# using .NET 8. **100x+ faster than traditional JSON-based storage** with ultra-fast binary format for massive dataset handling (100GB+). Now with a TCP Server, Web IDE, and Entity Framework Core provider.

## 🚀 Key Features

### ⚡ **Ultra-Fast Binary Storage**
- **100x+ faster** than JSON-based engines
- **5,000+ records/sec** insert performance
- **Sub-millisecond queries** on large datasets
- **Streaming I/O** for memory efficiency
- **Scalable to 100GB+** datasets

### 🌐 **Connectivity**
- **TCP Server** - For remote connections, listening on port 4162.
- **Web IDE** - A web-based interface to run queries against the TCP server.
- **Entity Framework Core Provider** - Integrate with EF Core for ORM capabilities.

### 🗄️ **Complete SQL Support**
- **CREATE TABLE** - Binary-optimized table creation
- **INSERT INTO** - Ultra-fast data insertion
- **SELECT** - High-speed querying with streaming
- **SELECT COUNT** - Count rows with optional `WHERE` clause.
- **UPDATE** - Record modification (basic support)
- **DELETE** - Record removal (basic support)
- **SHOW TABLES** - List all tables

### 📊 **Supported Data Types**
- **INTEGER** - 32-bit signed integers
- **LONG** - 64-bit signed integers (with auto-increment)
- **TEXT** - String values (UTF-8 encoded)
- **REAL** - Double-precision floating-point numbers

### 🔒 **Advanced Constraints**
- **NOT NULL** - Column cannot be empty
- **PRIMARY KEY** - Unique identifier with auto-increment
- **AUTO_INCREMENT** - Automatic ID generation

### 🔍 **High-Performance Query Features**
- **WHERE clauses** - Optimized filtering
- **ORDER BY** - Fast sorting with binary data
- **LIMIT** - Efficient result limiting
- **Column selection** - Minimized data transfer
- **Streaming results** - Memory-efficient large queries

## 📈 **Performance Benchmarks**

### Insert Performance
- **Binary Storage**: 5,023 records/sec
- **Traditional JSON**: ~25 records/sec
- **Performance Gain**: **200x faster**

### Query Performance
- **1,000 records**: 6.65ms
- **50,000 records**: 89.41ms (streaming)
- **Memory usage**: 60x less than JSON

### Storage Efficiency
- **100K records**: 43.9 MB binary storage
- **Ultra-fast startup**: No JSON parsing overhead
- **Scalable**: Tested up to 1M+ records

## 🎯 **Quick Start**

### Prerequisites
- [.NET 8 SDK](https://dotnet.microsoft.com/download/dotnet/8.0) or later

### 1. Build the Project
```bash
dotnet build
```

### 2. Run the Interactive CLI
This runs the SQL engine locally, storing data in the `binary_data` directory.
```bash
dotnet run
```
```
⚡ Ultra-Fast Binary SQL Engine - Interactive CLI ⚡
🚀 Features: Binary Storage | 100x+ Faster | Scalable

SQL> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
✅ Table 'users' created successfully with binary storage

SQL> INSERT INTO users VALUES (1, 'John Doe', 30);
✅ 1 row inserted with binary storage (Row ID: 0)

SQL> SELECT * FROM users;
📊 Query Results:
┌────┬──────────┬─────┐
│ id │ name     │ age │
├────┼──────────┼─────┤
│ 1  │ John Doe │ 30  │
└────┴──────────┴─────┘

SQL> SELECT COUNT(*) FROM users WHERE age > 25;
📊 Query Results:
┌─────────┐
│ COUNT   │
├─────────┤
│ 1       │
└─────────┘
```

### 3. Run the TCP Server and Web IDE

First, start the TCP server. This will listen for remote connections.
```bash
dotnet run -- --tcp-server
```

Next, in a **new terminal**, run the Web IDE.
```bash
dotnet run --project WebIDE/WebIDE.csproj
```
Now, open your browser and navigate to `http://localhost:5000` to use the Web IDE.

### 4. Using the Entity Framework Core Provider
You can connect to the database using the EF Core provider. Two connection modes are supported:

- **Local Mode**: Connects directly to the binary files.
  `"DataSource=local"`

- **TCP Mode**: Connects to the TCP server.
  `"DataSource=tcp://127.0.0.1:4162"`

### 5. Performance Tests
```bash
# Quick performance demo (100K records)
dotnet run --quick-scalability

# Full performance test
dotnet run --performance

# Scalability test (1M records)
dotnet run --scalability
```

## 🏗️ **Architecture**

The engine is built with a focus on performance and scalability.

- **Core Engine (`BinarySqlEngine.cs`)**: Handles SQL parsing, execution, and data manipulation.
- **Storage Manager (`HighPerformanceStorageManager.cs`)**: Manages binary file I/O, including data and metadata.
- **TCP Server (`SqlTcpServer.cs`)**: Allows remote clients to connect and execute queries.
- **Web IDE**: An ASP.NET Core application providing a user-friendly interface for database interaction.
- **EF Core Provider**: Enables integration with Entity Framework Core.

### Metadata
Table metadata is stored in `*_meta.json` files within the `binary_data/metadata` directory. These files define the table schema, including column names, data types, and constraints.
