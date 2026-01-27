// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

// ============================================================================
// Raycoon.Serilog.Sinks.SQLite - Sample Console Application
// ============================================================================
//
// This demo showcases the various configuration options of the SQLite sink:
//
// 1. BASIC CONFIGURATION
//    - Minimal setup using default values
//    - Only database path is required
//
// 2. ADVANCED CONFIGURATION
//    - Custom columns for efficient SQL queries
//    - Retention policies (time, count, size-based)
//    - Performance tuning (JournalMode, SynchronousMode, batching)
//    - Error callbacks
//
// 3. EXCEPTION LOGGING
//    - Nested exceptions with full stack traces
//    - Configurable exception length limits
//
// 4. HIGH-VOLUME LOGGING
//    - Parallel writes with optimized batch settings
//    - Queue limits for back-pressure handling
//
// 5. JSON CONFIGURATION
//    - Integration with Microsoft.Extensions.Configuration
//    - Minimum level overrides per namespace
//
// Available Configuration Options (SQLiteSinkOptions):
// ────────────────────────────────────────────────────
// DatabasePath          - Path to SQLite file (relative, absolute, or :memory:)
// TableName             - Name of the log table (default: "Logs")
// RestrictedToMinimumLevel - Sink-specific minimum level filter
// StoreTimestampInUtc   - UTC vs. local time (default: true)
//
// Batching:
// BatchSizeLimit        - Max events per batch (default: 100)
// BatchPeriod           - Interval between batch writes (default: 2s)
// QueueLimit            - Max events in queue (default: 10000)
//
// Retention:
// RetentionPeriod       - Max age of logs (e.g., 7 days)
// RetentionCount        - Max number of logs (e.g., 10000)
// MaxDatabaseSize       - Max file size in bytes
// CleanupInterval       - Interval for cleanup runs (default: 1 hour)
//
// SQLite Performance:
// JournalMode           - Delete, Truncate, Persist, Memory, Wal (default), Off
// SynchronousMode       - Off, Normal (default), Full, Extra
//
// Truncation:
// MaxMessageLength      - Max length of log messages
// MaxExceptionLength    - Max length of exception details
// MaxPropertiesLength   - Max length of JSON properties string
//
// Custom Columns:
// CustomColumns.Add()   - Extract properties into dedicated columns for SQL queries
//
// Error Handling:
// OnError               - Callback for write errors
// ThrowOnError          - Throw exceptions instead of suppressing (default: false)
// ============================================================================

using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Raycoon.Serilog.Sinks.SQLite.Options;
using Serilog;
using Serilog.Context;

Console.WriteLine("=== Serilog SQLite Sink Demo ===\n");

// ────────────────────────────────────────────────────────────────────────────
// EXAMPLE 1: Basic Configuration
// ────────────────────────────────────────────────────────────────────────────
// Minimal setup - only the database path is required.
// All other settings use sensible defaults:
// - TableName: "Logs"
// - BatchSizeLimit: 100, BatchPeriod: 2 seconds
// - JournalMode: WAL (best performance for concurrent access)
// - SynchronousMode: Normal (good balance between speed and safety)
// ────────────────────────────────────────────────────────────────────────────
Console.WriteLine("Example 1: Basic Configuration");
Console.WriteLine(new string('-', 40));

await using (var simpleLogger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console(formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/simple.db")
    .CreateLogger())
{
    simpleLogger.Information("This is a simple log message");
    simpleLogger.Debug("Debug information: Value = {Value}", 42);
    simpleLogger.Warning("A warning with timestamp");

    await Task.Delay(500);
}

Console.WriteLine("Logs written to 'logs/simple.db'\n");

// ────────────────────────────────────────────────────────────────────────────
// EXAMPLE 2: Advanced Configuration
// ────────────────────────────────────────────────────────────────────────────
// Demonstrates all important configuration options:
//
// RETENTION POLICIES (automatic cleanup):
// - RetentionPeriod: Delete logs older than 7 days
// - RetentionCount: Keep maximum 10,000 entries
// - MaxDatabaseSize: Limit database to 50 MB
//
// BATCHING (performance optimization):
// - BatchSizeLimit: 50 events per batch
// - BatchPeriod: Write every 500ms
//
// SQLITE TUNING:
// - JournalMode.Wal: Write-Ahead-Logging for better concurrency
// - SynchronousMode.Normal: Good balance between speed and durability
//
// CUSTOM COLUMNS:
// - Extract properties into dedicated columns for efficient SQL queries
// - CreateIndex: true automatically creates an index for fast lookups
// ────────────────────────────────────────────────────────────────────────────
Console.WriteLine("Example 2: Advanced Configuration");
Console.WriteLine(new string('-', 40));

await using (var advancedLogger = new LoggerConfiguration()
    .MinimumLevel.Verbose()
    .Enrich.FromLogContext()
    .Enrich.WithThreadId()
    .Enrich.WithMachineName()
    .WriteTo.Console(
        outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
        formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/advanced.db", options =>
    {
        // Table configuration
        options.TableName = "ApplicationLogs";

        // Retention policies - oldest logs are deleted when limits are exceeded
        options.RetentionPeriod = TimeSpan.FromDays(7);
        options.RetentionCount = 10000;
        options.MaxDatabaseSize = 50 * 1024 * 1024; // 50 MB

        // Batching configuration for throughput optimization
        options.BatchSizeLimit = 50;
        options.BatchPeriod = TimeSpan.FromMilliseconds(500);

        // SQLite performance settings
        options.JournalMode = SQLiteJournalMode.Wal;
        options.SynchronousMode = SQLiteSynchronousMode.Normal;

        // Custom columns for structured data - enables efficient SQL WHERE clauses
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "UserId",
            DataType = "TEXT",
            PropertyName = "UserId",
            CreateIndex = true  // Index for fast user lookups
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
            PropertyName = "Duration"
        });

        // Error callback - invoked on write failures
        options.OnError = ex => Console.WriteLine($"SQLite error: {ex.Message}");
    })
    .CreateLogger())
{
    // Simulate various log scenarios
    var random = new Random();
    var userIds = new[] { "user1", "user2", "user3", "admin" };
    var actions = new[] { "Login", "Logout", "ViewPage", "UpdateProfile", "Purchase" };

    for (var i = 0; i < 20; i++)
    {
        var userId = userIds[random.Next(userIds.Length)];
        var action = actions[random.Next(actions.Length)];
        var duration = random.NextDouble() * 1000;
        var requestId = Guid.NewGuid().ToString("N")[..8];

        // Use LogContext to add properties that map to custom columns
        using (LogContext.PushProperty("UserId", userId))
        using (LogContext.PushProperty("RequestId", requestId))
        using (LogContext.PushProperty("Duration", duration))
        {
            if (random.NextDouble() < 0.1) // 10% errors
            {
                advancedLogger.Error(
                    new InvalidOperationException($"Action '{action}' failed"),
                    "Error during action {Action} for user {UserId}",
                    action, userId);
            }
            else if (random.NextDouble() < 0.2) // 20% warnings
            {
                advancedLogger.Warning(
                    "Slow action {Action} - Duration: {Duration:F2}ms",
                    action, duration);
            }
            else
            {
                advancedLogger.Information(
                    "User {UserId} performed action {Action} - Duration: {Duration:F2}ms",
                    userId, action, duration);
            }
        }

        await Task.Delay(50);
    }

    await Task.Delay(1000);
}

Console.WriteLine("Logs written to 'logs/advanced.db'\n");

// ────────────────────────────────────────────────────────────────────────────
// EXAMPLE 3: Exception Logging
// ────────────────────────────────────────────────────────────────────────────
// Demonstrates storing exceptions with full details:
//
// - StoreExceptionDetails: true (default) stores Type, Message, StackTrace
// - MaxExceptionLength: 8000 limits the exception string length
//   (useful for very deep stack traces or AggregateExceptions)
//
// The Exception column contains the complete ToString() output including
// InnerExceptions and AggregateException contents.
// ────────────────────────────────────────────────────────────────────────────
Console.WriteLine("Example 3: Exception Logging");
Console.WriteLine(new string('-', 40));

await using (var exceptionLogger = new LoggerConfiguration()
    .WriteTo.Console(formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/exceptions.db", options =>
    {
        options.TableName = "Exceptions";
        options.StoreExceptionDetails = true;
        options.MaxExceptionLength = 8000;
    })
    .CreateLogger())
{
    try
    {
        await SimulateComplexException();
    }
    catch (Exception ex)
    {
        exceptionLogger.Error(ex, "A complex error occurred");
    }

    await Task.Delay(500);
}

Console.WriteLine("Logs written to 'logs/exceptions.db'\n");

// ────────────────────────────────────────────────────────────────────────────
// EXAMPLE 4: High-Volume Logging (Performance Test)
// ────────────────────────────────────────────────────────────────────────────
// Optimized settings for high throughput:
//
// - BatchSizeLimit: 500 (larger batches = fewer transactions)
// - BatchPeriod: 100ms (more frequent writes = lower latency)
// - QueueLimit: 50,000 (larger buffer for burst traffic)
//
// BACK-PRESSURE:
// When the queue is full (> QueueLimit), new events are dropped.
// This prevents memory exhaustion under sustained overload.
//
// This test writes 5,000 events in parallel with 10 threads and measures
// throughput (events/second).
// ────────────────────────────────────────────────────────────────────────────
Console.WriteLine("Example 4: High-Volume Logging (Performance Test)");
Console.WriteLine(new string('-', 40));

var stopwatch = Stopwatch.StartNew();
const int messageCount = 5000;

await using (var highVolumeLogger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/highvolume.db", options =>
    {
        options.BatchSizeLimit = 500;
        options.BatchPeriod = TimeSpan.FromMilliseconds(100);
        options.QueueLimit = 50000;
    })
    .CreateLogger())
{
    // Parallel logging from multiple threads
    await Parallel.ForEachAsync(
        Enumerable.Range(0, messageCount),
        new ParallelOptions { MaxDegreeOfParallelism = 10 },
        async (i, ct) =>
        {
            highVolumeLogger.Information(
                "High-Volume Message {MessageId}: Data={Data}, Timestamp={Timestamp}",
                i,
                Guid.NewGuid(),
                DateTime.UtcNow);

            if (i % 1000 == 0)
            {
                await Task.Yield();
            }
        });

    await Task.Delay(2000); // Wait for flush
}

stopwatch.Stop();
Console.WriteLine($"{messageCount:N0} messages written in {stopwatch.ElapsedMilliseconds}ms");
Console.WriteLine($"  Throughput: {messageCount / stopwatch.Elapsed.TotalSeconds:N0} msgs/sec\n");

// ────────────────────────────────────────────────────────────────────────────
// EXAMPLE 5: JSON Configuration
// ────────────────────────────────────────────────────────────────────────────
// Integration with Microsoft.Extensions.Configuration:
//
// - Read minimum levels from appsettings.json
// - Override levels per namespace (e.g., suppress Microsoft.* noise)
// - Combine with programmatic SQLite sink configuration
//
// Note: The SQLite sink itself can also be configured via JSON using
// Serilog.Settings.Configuration, but this example shows the hybrid approach.
// ────────────────────────────────────────────────────────────────────────────
Console.WriteLine("Example 5: JSON Configuration");
Console.WriteLine(new string('-', 40));

// Create a temporary appsettings.json
var appSettingsContent = """
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft": "Warning",
        "System": "Warning"
      }
    }
  }
}
""";

await File.WriteAllTextAsync("appsettings.json", appSettingsContent);

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .Build();

await using (var configLogger = new LoggerConfiguration()
    .ReadFrom.Configuration(configuration)
    .WriteTo.Console(formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/configured.db", options =>
    {
        options.TableName = "ConfiguredLogs";
    })
    .CreateLogger())
{
    configLogger.Information("Log from JSON-configured logger");
    configLogger.Debug("This debug message is filtered (MinLevel=Information)");

    await Task.Delay(500);
}

Console.WriteLine("Logs written via JSON-configured logger\n");

// ────────────────────────────────────────────────────────────────────────────
// Summary
// ────────────────────────────────────────────────────────────────────────────
Console.WriteLine("=== Summary ===");
Console.WriteLine(new string('=', 40));

var logFiles = Directory.GetFiles("logs", "*.db")
    .Select(f => new FileInfo(f))
    .OrderBy(f => f.Name);

foreach (var file in logFiles)
{
    Console.WriteLine($"  {file.Name,-25} {file.Length / 1024.0:F1} KB");
}

Console.WriteLine("\nDemo completed!");

// ────────────────────────────────────────────────────────────────────────────
// Helper method for complex exception simulation
// ────────────────────────────────────────────────────────────────────────────
static async Task SimulateComplexException()
{
    try
    {
        await Task.Run(() =>
        {
            try
            {
                throw new InvalidOperationException("Inner error in task");
            }
            catch (Exception inner)
            {
                throw new AggregateException("Multiple errors occurred",
                    inner,
                    new ArgumentException("Invalid argument"),
                    new TimeoutException("Operation timed out"));
            }
        });
    }
    catch (Exception ex)
    {
        throw new ApplicationException("Application error with nested exceptions", ex);
    }
}
