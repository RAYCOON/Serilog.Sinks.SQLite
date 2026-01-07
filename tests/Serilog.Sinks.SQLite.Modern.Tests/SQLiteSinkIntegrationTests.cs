// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using Microsoft.Data.Sqlite;
using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Tests;

public sealed class SQLiteSinkIntegrationTests : IDisposable
{
    private readonly string _testDbPath;
    private bool _disposed;

    public SQLiteSinkIntegrationTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"serilog_test_{Guid.NewGuid()}.db");
    }

    [Fact]
    public async Task WriteToSQLiteShouldCreateDatabaseAndTable()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test message");

            // Wait for batch to flush before dispose
            await Task.Delay(200);
        }

        // Additional delay after dispose to ensure all writes complete
        await Task.Delay(100);

        // Assert
        File.Exists(_testDbPath).Should().BeTrue();

        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='Logs'";
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be("Logs");
    }

    [Fact]
    public async Task WriteToSQLiteShouldWriteLogEvents()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test message {Value}", 42);
            logger.Warning("Warning message");
            logger.Error(new InvalidOperationException("Test error"), "Error occurred");

            // Wait for batch to flush - must be longer than batchPeriod
            await Task.Delay(300);
        }

        // Additional delay after dispose
        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var countCmd = connection.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await countCmd.ExecuteScalarAsync())!;
        count.Should().Be(3);

        await using var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT Level, LevelName, Message FROM Logs ORDER BY Id";
        await using var reader = await selectCmd.ExecuteReaderAsync();

        // First log (Information)
        await reader.ReadAsync();
        reader.GetInt32(0).Should().Be((int)LogEventLevel.Information);
        reader.GetString(1).Should().Be("Information");
        reader.GetString(2).Should().Contain("42");

        // Second log (Warning)
        await reader.ReadAsync();
        reader.GetInt32(0).Should().Be((int)LogEventLevel.Warning);

        // Third log (Error)
        await reader.ReadAsync();
        reader.GetInt32(0).Should().Be((int)LogEventLevel.Error);
    }

    [Fact]
    public async Task WriteToSQLiteWithCustomTableNameShouldUseCustomTable()
    {
        // Arrange
        const string customTableName = "CustomLogs";

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, tableName: customTableName, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {customTableName}";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().BeGreaterThanOrEqualTo(1);
    }

    [Fact]
    public async Task WriteToSQLiteWithUTCTimestampShouldStoreUTC()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, storeTimestampInUtc: true, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Timestamp FROM Logs LIMIT 1";
        var timestampStr = (string)(await cmd.ExecuteScalarAsync())!;

        // ISO 8601 format should contain 'Z' or '+00:00' for UTC
        var isUtcFormat = timestampStr.Contains('Z', StringComparison.Ordinal) ||
                          timestampStr.Contains("+00:00", StringComparison.Ordinal);
        isUtcFormat.Should().BeTrue("Timestamp should be in UTC format");
    }

    [Fact]
    public async Task WriteToSQLiteShouldStoreException()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            try
            {
                throw new InvalidOperationException("Test exception message");
            }
            catch (InvalidOperationException ex)
            {
                logger.Error(ex, "An error occurred");
            }
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Exception FROM Logs WHERE Exception IS NOT NULL LIMIT 1";
        var exception = (string?)(await cmd.ExecuteScalarAsync());

        exception.Should().NotBeNullOrEmpty();
        exception.Should().Contain("InvalidOperationException");
        exception.Should().Contain("Test exception message");
    }

    [Fact]
    public async Task WriteToSQLiteShouldStorePropertiesAsJson()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("User {UserId} performed {Action}", 123, "Login");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Properties FROM Logs LIMIT 1";
        var properties = (string?)(await cmd.ExecuteScalarAsync());

        properties.Should().NotBeNullOrEmpty();
        properties.Should().Contain("UserId");
        properties.Should().Contain("123");
        properties.Should().Contain("Action");
        properties.Should().Contain("Login");
    }

    [Fact]
    public async Task WriteToSQLiteWithCustomColumnsShouldCreateAndPopulate()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.CustomColumns.Add(new CustomColumn
                {
                    ColumnName = "UserId",
                    DataType = "TEXT",
                    PropertyName = "UserId"
                });
                options.CustomColumns.Add(new CustomColumn
                {
                    ColumnName = "RequestId",
                    DataType = "TEXT",
                    PropertyName = "RequestId",
                    CreateIndex = true
                });
            })
            .CreateLogger())
        {
            logger
                .ForContext("UserId", "user123")
                .ForContext("RequestId", "req456")
                .Information("Custom column test");

            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT UserId, RequestId FROM Logs LIMIT 1";
        await using var reader = await cmd.ExecuteReaderAsync();
        await reader.ReadAsync();

        reader.GetString(0).Should().Be("user123");
        reader.GetString(1).Should().Be("req456");
    }

    [Fact]
    public async Task WriteToSQLiteWithMinimumLevelShouldFilterEvents()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.SQLite(_testDbPath,
                restrictedToMinimumLevel: LogEventLevel.Warning,
                batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Debug("Debug message");
            logger.Information("Info message");
            logger.Warning("Warning message");
            logger.Error("Error message");

            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await cmd.ExecuteScalarAsync())!;

        count.Should().Be(2); // Only Warning and Error
    }

    [Fact]
    public async Task WriteToSQLiteInParallelShouldHandleConcurrency()
    {
        // Arrange
        const int taskCount = 10;
        const int messagesPerTask = 100;

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.BatchSizeLimit = 50;
            })
            .CreateLogger())
        {
            var tasks = Enumerable.Range(0, taskCount)
                .Select(taskId => Task.Run(() =>
                {
                    for (var i = 0; i < messagesPerTask; i++)
                    {
                        logger.Information("Task {TaskId} Message {MessageId}", taskId, i);
                    }
                }))
                .ToArray();

            await Task.WhenAll(tasks);

            // Wait for all batches to flush
            await Task.Delay(3000);
        }

        // Additional delay after dispose
        await Task.Delay(500);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await cmd.ExecuteScalarAsync())!;

        count.Should().Be(taskCount * messagesPerTask);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Cleanup test database
        DeleteFileIfExists(_testDbPath);
        DeleteFileIfExists(_testDbPath + "-wal");
        DeleteFileIfExists(_testDbPath + "-shm");

        GC.SuppressFinalize(this);
    }

    private static void DeleteFileIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
            // Ignore cleanup errors - file may be locked
        }
        catch (UnauthorizedAccessException)
        {
            // Ignore cleanup errors - no permissions
        }
    }
}
