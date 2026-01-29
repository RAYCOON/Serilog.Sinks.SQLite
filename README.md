# Raycoon.Serilog.Sinks.SQLite

[![NuGet](https://img.shields.io/nuget/v/Raycoon.Serilog.Sinks.SQLite.svg)](https://www.nuget.org/packages/Raycoon.Serilog.Sinks.SQLite)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0%20%7C%2010.0-512BD4)](https://dotnet.microsoft.com/)

A modern, high-performance Serilog sink for SQLite databases. Developed for .NET 8+ with full **AnyCPU** support.

## Features

- **AnyCPU compatible** - Uses `Microsoft.Data.Sqlite` (no native SQLite required)
- **.NET 8.0, .NET 9.0 & .NET 10.0** support
- **Asynchronous batching** - Optimal performance through batch writing
- **Automatic retention** - By time, count, or database size
- **Custom columns** - Store structured data in dedicated columns
- **WAL mode** - Optimized for high write load
- **Thread-safe** - Fully suitable for parallel logging
- **Configurable** - Extensive options for every use case

## Installation

```bash
dotnet add package Raycoon.Serilog.Sinks.SQLite
```

## Quick Start

### Basic Usage

```csharp
using Serilog;

var logger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/app.db")
    .CreateLogger();

logger.Information("Hello, SQLite!");
logger.Error(new Exception("Oops!"), "An error occurred");

// Important: Dispose the logger at the end
await Log.CloseAndFlushAsync();
```

### Advanced Configuration

```csharp
using Serilog;
using Serilog.Events;
using Raycoon.Serilog.Sinks.SQLite.Options;

var logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .Enrich.FromLogContext()
    .Enrich.WithThreadId()
    .WriteTo.SQLite("logs/app.db", options =>
    {
        // Table name
        options.TableName = "ApplicationLogs";

        // Retention: Delete logs older than 30 days
        options.RetentionPeriod = TimeSpan.FromDays(30);

        // Retention: Keep maximum 100,000 entries
        options.RetentionCount = 100_000;

        // Retention: Database max. 100 MB
        options.MaxDatabaseSize = 100 * 1024 * 1024;

        // Performance tuning
        options.BatchSizeLimit = 200;
        options.BatchPeriod = TimeSpan.FromSeconds(1);
        options.QueueLimit = 50000;

        // SQLite optimizations
        options.JournalMode = SQLiteJournalMode.Wal;
        options.SynchronousMode = SQLiteSynchronousMode.Normal;

        // Store timestamps in UTC
        options.StoreTimestampInUtc = true;

        // Minimum log level for this sink
        options.RestrictedToMinimumLevel = LogEventLevel.Information;
    })
    .CreateLogger();
```

### Custom Columns

Store structured data in dedicated columns for better queries:

```csharp
var logger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/app.db", options =>
    {
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "UserId",
            DataType = "TEXT",
            PropertyName = "UserId",
            CreateIndex = true // Index for fast searches
        });

        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "RequestId",
            DataType = "TEXT",
            PropertyName = "RequestId"
        });

        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "Duration",
            DataType = "REAL",
            PropertyName = "DurationMs"
        });
    })
    .CreateLogger();

// Usage
logger
    .ForContext("UserId", "user123")
    .ForContext("RequestId", Guid.NewGuid())
    .ForContext("DurationMs", 42.5)
    .Information("Request processed");
```

### Error Handling

```csharp
var logger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/app.db", options =>
    {
        options.OnError = ex =>
        {
            Console.WriteLine($"SQLite Error: {ex.Message}");
            // Or: Use a fallback logger
        };

        // Throw exception on critical errors
        options.ThrowOnError = false; // Default: false
    })
    .CreateLogger();
```

### JSON Configuration (appsettings.json)

The sink supports full configuration via `appsettings.json` using `Serilog.Settings.Configuration`:

```bash
dotnet add package Serilog.Settings.Configuration
dotnet add package Microsoft.Extensions.Configuration.Json
```

#### Basic JSON Configuration

```json
{
  "Serilog": {
    "Using": ["Raycoon.Serilog.Sinks.SQLite"],
    "MinimumLevel": "Information",
    "WriteTo": [
      {
        "Name": "SQLite",
        "Args": {
          "databasePath": "logs/app.db"
        }
      }
    ]
  }
}
```

#### Full JSON Configuration

```json
{
  "Serilog": {
    "Using": ["Raycoon.Serilog.Sinks.SQLite"],
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft": "Warning",
        "System": "Warning"
      }
    },
    "WriteTo": [
      {
        "Name": "SQLite",
        "Args": {
          "databasePath": "logs/app.db",
          "tableName": "ApplicationLogs",
          "restrictedToMinimumLevel": "Information",
          "storeTimestampInUtc": true,
          "autoCreateDatabase": true,
          "storePropertiesAsJson": true,
          "storeExceptionDetails": true,
          "maxMessageLength": 10000,
          "maxExceptionLength": 20000,
          "maxPropertiesLength": 10000,
          "batchSizeLimit": 200,
          "batchPeriod": "00:00:01",
          "queueLimit": 50000,
          "retentionPeriod": "30.00:00:00",
          "retentionCount": 100000,
          "maxDatabaseSize": 104857600,
          "cleanupInterval": "01:00:00",
          "journalMode": "Wal",
          "synchronousMode": "Normal",
          "throwOnError": false,
          "customColumns": [
            {
              "columnName": "UserId",
              "dataType": "TEXT",
              "propertyName": "UserId",
              "allowNull": true,
              "createIndex": true
            },
            {
              "columnName": "RequestId",
              "dataType": "TEXT",
              "propertyName": "RequestId",
              "allowNull": true,
              "createIndex": false
            },
            {
              "columnName": "Duration",
              "dataType": "REAL",
              "propertyName": "DurationMs",
              "allowNull": true,
              "createIndex": false
            }
          ]
        }
      }
    ],
    "Enrich": ["FromLogContext"]
  }
}
```

#### C# Setup for JSON Configuration

```csharp
using Microsoft.Extensions.Configuration;
using Serilog;

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json")
    .Build();

var logger = new LoggerConfiguration()
    .ReadFrom.Configuration(configuration)
    .CreateLogger();
```

#### TimeSpan Format in JSON

| Format | Example | Description |
|--------|---------|-------------|
| `hh:mm:ss` | `"00:00:02"` | 2 seconds |
| `hh:mm:ss.fff` | `"00:00:00.500"` | 500 milliseconds |
| `d.hh:mm:ss` | `"7.00:00:00"` | 7 days |
| `d.hh:mm:ss` | `"30.00:00:00"` | 30 days |

#### Enum Values in JSON

| Property | Valid Values |
|----------|-------------|
| `journalMode` | `"Delete"`, `"Truncate"`, `"Persist"`, `"Memory"`, `"Wal"`, `"Off"` |
| `synchronousMode` | `"Off"`, `"Normal"`, `"Full"`, `"Extra"` |
| `restrictedToMinimumLevel` | `"Verbose"`, `"Debug"`, `"Information"`, `"Warning"`, `"Error"`, `"Fatal"` |

#### Limitations

The following options are **not available** via JSON configuration:
- `OnError` callback (delegates cannot be serialized)
- `AdditionalConnectionParameters` (dictionary binding is complex)

Use programmatic configuration for these features.

## Database Schema

The sink automatically creates the following table:

| Column | Type | Description |
|--------|------|-------------|
| `Id` | INTEGER | Primary key (auto-increment) |
| `Timestamp` | TEXT | ISO 8601 timestamp |
| `Level` | INTEGER | Log level (0-5) |
| `LevelName` | TEXT | Log level name |
| `Message` | TEXT | Rendered message |
| `MessageTemplate` | TEXT | Original message template |
| `Exception` | TEXT | Exception details (if present) |
| `Properties` | TEXT | Properties as JSON |
| `SourceContext` | TEXT | Logger name / source |
| `MachineName` | TEXT | Computer name |
| `ThreadId` | INTEGER | Thread ID |

Plus all configured custom columns.

## Querying Logs

```sql
-- All errors from the last 24 hours
SELECT * FROM Logs
WHERE Level >= 4
AND Timestamp > datetime('now', '-1 day')
ORDER BY Timestamp DESC;

-- Logs by UserId (if custom column configured)
SELECT * FROM Logs
WHERE UserId = 'user123'
ORDER BY Timestamp DESC
LIMIT 100;

-- Aggregation by level
SELECT LevelName, COUNT(*) as Count
FROM Logs
GROUP BY Level;

-- Search through properties (JSON)
SELECT * FROM Logs
WHERE json_extract(Properties, '$.RequestId') = 'abc123';
```

## Performance Tips

### 1. Optimize Batch Size

```csharp
options.BatchSizeLimit = 500;  // For high-volume
options.BatchPeriod = TimeSpan.FromMilliseconds(100);
```

### 2. Use WAL Mode (Default)

```csharp
options.JournalMode = SQLiteJournalMode.Wal;
```

### 3. Adjust Synchronous Mode

```csharp
// Faster, but less safe in case of power failure
options.SynchronousMode = SQLiteSynchronousMode.Normal;

// Or for maximum performance (only if data loss is acceptable)
options.SynchronousMode = SQLiteSynchronousMode.Off;
```

### 4. Set Queue Limit

```csharp
// Prevents memory overflow during burst traffic
options.QueueLimit = 100000;
```

## Comparison to Other SQLite Sinks

| Feature | Raycoon.Serilog.Sinks.SQLite | Serilog.Sinks.SQLite |
|---------|----------------------------|---------------------|
| AnyCPU Support | Yes (Microsoft.Data.Sqlite) | No (System.Data.SQLite) |
| .NET 8/9/10 | Yes | Partial (.NET 7 only) |
| Async Batching | Yes | Yes |
| Retention Policies | Yes (time, count, size) | No |
| Custom Columns | Yes | No |
| WAL Mode | Yes | Yes |

## API Reference

### SQLiteSinkOptions

#### Database

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DatabasePath` | string | "logs.db" | Path to the SQLite database file |
| `TableName` | string | "Logs" | Name of the log table |
| `AutoCreateDatabase` | bool | true | Auto-create database file if not exists |

#### Logging Behavior

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RestrictedToMinimumLevel` | LogEventLevel | Verbose | Minimum log level to capture |
| `StoreTimestampInUtc` | bool | true | Store timestamps in UTC (false = local time) |
| `StorePropertiesAsJson` | bool | true | Store log event properties as JSON |
| `StoreExceptionDetails` | bool | true | Store exception details in separate column |

#### Data Limits

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxMessageLength` | int? | null | Max rendered message length (null = unlimited) |
| `MaxExceptionLength` | int? | null | Max exception text length (null = unlimited) |
| `MaxPropertiesLength` | int? | null | Max properties JSON length (null = unlimited) |

#### Batching & Performance

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BatchSizeLimit` | int | 100 | Max events per batch write |
| `BatchPeriod` | TimeSpan | 2s | Interval between batch writes |
| `QueueLimit` | int? | 10000 | Max events in memory queue (null = unlimited) |

#### Retention Policy

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RetentionPeriod` | TimeSpan? | null | Delete logs older than this (null = disabled) |
| `RetentionCount` | long? | null | Keep only this many logs (null = disabled) |
| `MaxDatabaseSize` | long? | null | Max database size in bytes (null = disabled) |
| `CleanupInterval` | TimeSpan | 1h | Interval for retention cleanup checks |

#### SQLite Configuration

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `JournalMode` | SQLiteJournalMode | Wal | SQLite journal mode (Wal recommended) |
| `SynchronousMode` | SQLiteSynchronousMode | Normal | SQLite synchronous mode |
| `AdditionalConnectionParameters` | Dictionary | {} | Extra SQLite connection string parameters |

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## Contributing

Pull requests are welcome! Please open an issue first to discuss proposed changes.

## Changelog

### 1.0.0

- Initial Release
- .NET 8.0, .NET 9.0 and .NET 10.0 support
- AnyCPU compatibility with Microsoft.Data.Sqlite
- Async batching
- Retention policies (time, count, size)
- Custom columns
- WAL mode support
