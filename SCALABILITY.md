# Scalability Improvements for Large Datasets (100GB+)

## Problem with Original Implementation

The original SQL engine loads all table data into memory using `List<Dictionary<string, object?>>` in the `TableFileData.Rows` property. This approach has several limitations:

### Memory Issues
- **100GB+ datasets won't fit in RAM** on most systems
- **OutOfMemoryException** will occur with large datasets
- **Poor performance** due to garbage collection pressure
- **No memory locality** - all data loaded regardless of query needs

### Performance Issues
- **Slow startup** - entire table loaded on first access
- **Blocking operations** - must load all data before any query can run  
- **No streaming** - results must fit in memory
- **File I/O bottlenecks** - single large JSON file per table

## Scalable Solution

### 1. **Separate Data from Metadata**
```
Original: table.json (contains all rows + metadata)
Scalable: table_meta.json + table_data_000001.jsonl + table_data_000002.jsonl + ...
```

### 2. **Streaming Architecture**
- **Metadata only in memory** (~KB instead of GB)
- **Stream rows on-demand** from multiple data files
- **Lazy loading** - only read data when needed
- **Configurable buffer sizes** for optimal I/O

### 3. **File Organization**
```
data/
├── metadata/
│   ├── users_meta.json          # Table metadata (columns, stats, etc.)
│   └── orders_meta.json
├── tables/
│   ├── users_data_000001.jsonl  # 10K rows per file
│   ├── users_data_000002.jsonl
│   ├── orders_data_000001.jsonl
│   └── orders_data_000002.jsonl
└── indexes/
    ├── users_email.json         # Index files remain the same
    └── orders_user_id.json
```

### 4. **Key Improvements**

#### Memory Efficiency
```csharp
// Original: All rows in memory
List<Dictionary<string, object?>> allRows; // 100GB+ in RAM

// Scalable: Only metadata in memory  
ScalableTableMetadata metadata; // ~1KB in RAM
```

#### Streaming Queries
```csharp
// Original: Load all, then filter
var allRows = LoadAllRows(); // 100GB loaded
var results = allRows.Where(predicate).Take(100);

// Scalable: Stream and filter
var results = table.SelectRows(predicate: predicate, limit: 100);
// Only reads what's needed
```

#### Append-Only Writes
```csharp
// Original: Rewrite entire file
SaveAllRowsToSingleFile(allRows); // Slow for large datasets

// Scalable: Append to current file
AppendRowToCurrentDataFile(row); // Fast, no rewriting
```

## Performance Characteristics

| Operation | Original | Scalable | Improvement |
|-----------|----------|----------|-------------|
| **Memory Usage** | O(n) all rows | O(1) metadata only | ~1000x less |
| **Startup Time** | O(n) load all | O(1) load metadata | ~1000x faster |  
| **Insert** | O(n) rewrite file | O(1) append | ~100x faster |
| **Small Query** | O(n) scan all | O(k) scan needed | ~10-100x faster |
| **Large Query** | O(n) + memory limit | O(n) streaming | No memory limit |

## Usage Examples

### Performance Test
```bash
# Compare original vs scalable with 100K records
./SimpleSqlEngine --performance

# Test huge dataset handling (10M records)
./SimpleSqlEngine --scalability
```

### Code Examples

#### Handling Large Datasets
```csharp
// Create scalable table
var columns = new List<Column>
{
    new Column("id", DataType.Long, false, true, true),
    new Column("data", DataType.Text, false)
};

var table = new ScalableFileTable("huge_table", columns, storageManager);

// Insert millions of records (streams to disk)
for (int i = 0; i < 10_000_000; i++)
{
    table.AddRow(new Dictionary<string, object?> 
    { 
        ["data"] = $"Record {i}" 
    });
}

// Query with streaming (memory efficient)
var results = table.SelectRows(
    predicate: row => row["id"] > 1000000,
    limit: 1000
).ToList(); // Only 1000 rows in memory
```

#### Memory-Efficient Queries
```csharp
// Stream through entire dataset without loading into memory
foreach (var row in table.SelectRows())
{
    ProcessRow(row); // Process one row at a time
    // Previous rows are garbage collected
}
```

## Configuration Options

### ScalableFileStorageManager Parameters
```csharp
var storageManager = new ScalableFileStorageManager(
    baseDirectory: "data",    // Storage location
    rowsPerFile: 10000,      // Rows per data file (tune for your use case)
    bufferSize: 1024 * 1024  // I/O buffer size (1MB default)
);
```

### Tuning Recommendations

| Dataset Size | rowsPerFile | bufferSize | Expected Files |
|--------------|-------------|------------|----------------|
| < 1M rows | 50,000 | 512KB | < 20 files |
| 1M - 10M rows | 100,000 | 1MB | 10-100 files |
| 10M - 100M rows | 500,000 | 2MB | 20-200 files |
| 100M+ rows | 1,000,000 | 4MB | 100+ files |

## Limitations and Trade-offs

### What's Better
- ✅ **Memory usage**: Constant vs linear
- ✅ **Startup time**: Instant vs slow  
- ✅ **Insert performance**: Fast append vs full rewrite
- ✅ **Large dataset support**: 100GB+ vs ~1GB limit
- ✅ **Streaming queries**: No memory limit

### What's the Same
- ⚖️ **Query performance**: Similar for small result sets
- ⚖️ **Index performance**: Same index structure
- ⚖️ **SQL compatibility**: Same SQL features

### What's Slower
- ❌ **Updates**: Requires file rewriting (use sparingly)
- ❌ **Full table scans**: Slightly slower due to multiple files
- ❌ **Complex joins**: Not implemented (same as original)

## Migration Path

1. **Backward Compatible**: Original tables still work
2. **Gradual Migration**: Can use both implementations simultaneously
3. **Data Export/Import**: Use SQL commands to move data between implementations

## Future Enhancements

- **Compression**: Compress data files for better storage efficiency
- **Partitioning**: Date/range-based partitioning for better query performance  
- **Parallel I/O**: Multi-threaded reading for faster queries
- **Write-Ahead Logging**: Better crash recovery
- **Column Storage**: Store columns separately for analytical queries
