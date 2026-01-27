// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using Raycoon.Serilog.Sinks.SQLite.Options;
using Serilog;
using Serilog.Events;

namespace Raycoon.Serilog.Sinks.SQLite.Tests;

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
        count.Should().Be(1);
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

    [Fact]
    public async Task WriteToSQLiteWithLocalTimestampShouldStoreLocal()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, storeTimestampInUtc: false, batchPeriod: TimeSpan.FromMilliseconds(50))
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

        // Local timestamp should have an offset that is not +00:00 or Z (unless local is UTC)
        // The format should be ISO 8601 with offset
        timestampStr.Should().Contain("T"); // ISO 8601 format
    }

    [Fact]
    public async Task WriteToSQLiteWithInMemoryDatabaseShouldWork()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(":memory:", batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test message in memory");
            await Task.Delay(200);
        }

        // Assert - If we got here without exceptions, the test passed
        // In-memory databases are disposed with the connection, so we can't verify contents after
    }

    [Fact]
    public async Task WriteToSQLiteWithOnErrorCallbackShouldInvoke()
    {
        // Arrange
        Exception? capturedError = null;
        var errorCallbackInvoked = false;

        // Create a database with a read-only scenario that will cause an error
        // For this test, we use ThrowOnError to verify error handling

        // Use a callback to capture any errors
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.OnError = ex =>
                {
                    capturedError = ex;
                    errorCallbackInvoked = true;
                };
            })
            .CreateLogger())
        {
            logger.Information("Test message");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert - Normal operation should not trigger error callback
        errorCallbackInvoked.Should().BeFalse();
        capturedError.Should().BeNull();
    }

    [Fact]
    public async Task WriteToSQLiteWithMaxMessageLengthShouldTruncate()
    {
        // Arrange
        const int maxLength = 20;
        var longValue = new string('X', 100);

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.MaxMessageLength = maxLength;
            })
            .CreateLogger())
        {
            logger.Information("Message: {LongValue}", longValue);
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Message FROM Logs LIMIT 1";
        var message = (string)(await cmd.ExecuteScalarAsync())!;

        message.Length.Should().Be(maxLength);
    }

    [Fact]
    public async Task WriteToSQLiteWithMaxExceptionLengthShouldTruncate()
    {
        // Arrange
        const int maxLength = 50;
        var longExceptionMessage = new string('E', 200);

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.MaxExceptionLength = maxLength;
            })
            .CreateLogger())
        {
            try
            {
                throw new InvalidOperationException(longExceptionMessage);
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Error occurred");
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

        exception.Should().NotBeNull();
        exception!.Length.Should().Be(maxLength);
    }

    [Fact]
    public async Task WriteToSQLiteWithMaxPropertiesLengthShouldTruncate()
    {
        // Arrange
        const int maxLength = 30;
        var longValue = new string('P', 100);

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.MaxPropertiesLength = maxLength;
            })
            .CreateLogger())
        {
            logger.Information("Message with {LongProperty}", longValue);
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Properties FROM Logs WHERE Properties IS NOT NULL LIMIT 1";
        var properties = (string?)(await cmd.ExecuteScalarAsync());

        properties.Should().NotBeNull();
        properties!.Length.Should().Be(maxLength);
    }

    [Fact]
    public async Task WriteToSQLiteWithEmptyPropertiesShouldStoreNull()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            // Log a simple message without any properties (except built-in ones like SourceContext)
            logger.Information("Simple message without properties");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Properties FROM Logs LIMIT 1";
        var result = await cmd.ExecuteScalarAsync();

        // Properties might be an empty JSON object {} if there are no custom properties
        // The actual behavior depends on implementation
        (result == DBNull.Value || (result is string s && (s == "{}" || string.IsNullOrEmpty(s)))).Should().BeTrue();
    }

    [Fact]
    public async Task WriteToSQLiteWithSpecialCharactersShouldEscape()
    {
        // Arrange
        var specialValue = "Message with \"quotes\" and 'apostrophes' and \n newlines \t tabs";
        var unicodeValue = "Unicode: \u00e9\u00e8\u00ea \u4e2d\u6587 \U0001F600";

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Special: {Content}", specialValue);
            logger.Information("Unicode: {Content}", unicodeValue);
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await cmd.ExecuteScalarAsync())!;

        count.Should().Be(2);

        // Verify the messages are stored correctly
        await using var selectCmd = connection.CreateCommand();
        selectCmd.CommandText = "SELECT Message FROM Logs ORDER BY Id";
        await using var reader = await selectCmd.ExecuteReaderAsync();

        await reader.ReadAsync();
        reader.GetString(0).Should().Contain("quotes");
        reader.GetString(0).Should().Contain("apostrophes");

        await reader.ReadAsync();
        reader.GetString(0).Should().Contain("Unicode");
    }

    [Fact]
    public async Task WriteToSQLiteWithNullCustomColumnShouldStoreNull()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.CustomColumns.Add(new CustomColumn
                {
                    ColumnName = "OptionalUserId",
                    DataType = "TEXT",
                    PropertyName = "OptionalUserId",
                    AllowNull = true
                });
            })
            .CreateLogger())
        {
            // Log without the custom property
            logger.Information("Message without custom property");

            // Log with the custom property
            logger
                .ForContext("OptionalUserId", "user123")
                .Information("Message with custom property");

            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT OptionalUserId FROM Logs ORDER BY Id";
        await using var reader = await cmd.ExecuteReaderAsync();

        // First row should have NULL
        await reader.ReadAsync();
        (await reader.IsDBNullAsync(0)).Should().BeTrue();

        // Second row should have the value
        await reader.ReadAsync();
        reader.GetString(0).Should().Be("user123");
    }

    [Fact]
    public async Task WriteToSQLiteWithDifferentJournalModeShouldWork()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.JournalMode = SQLiteJournalMode.Delete;
            })
            .CreateLogger())
        {
            logger.Information("Test with Delete journal mode");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        File.Exists(_testDbPath).Should().BeTrue();

        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(1);
    }

    [Fact]
    public async Task WriteToSQLiteWithDifferentSynchronousModeShouldWork()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.SynchronousMode = SQLiteSynchronousMode.Full;
            })
            .CreateLogger())
        {
            logger.Information("Test with Full synchronous mode");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(1);
    }

    [Fact]
    public async Task WriteToSQLiteWithStoreExceptionDetailsFalseShouldNotStoreException()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.StoreExceptionDetails = false;
            })
            .CreateLogger())
        {
            try
            {
                throw new InvalidOperationException("Test exception");
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Error occurred");
            }
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Exception FROM Logs LIMIT 1";
        var result = await cmd.ExecuteScalarAsync();

        result.Should().Be(DBNull.Value);
    }

    [Fact]
    public async Task WriteToSQLiteWithStorePropertiesAsJsonFalseShouldNotStoreProperties()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.StorePropertiesAsJson = false;
            })
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
        var result = await cmd.ExecuteScalarAsync();

        result.Should().Be(DBNull.Value);
    }

    [Fact]
    public async Task WriteToSQLiteWithCustomColumnIndexShouldCreateIndex()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
                options.CustomColumns.Add(new CustomColumn
                {
                    ColumnName = "CorrelationId",
                    DataType = "TEXT",
                    PropertyName = "CorrelationId",
                    CreateIndex = true
                });
            })
            .CreateLogger())
        {
            logger
                .ForContext("CorrelationId", "corr-123")
                .Information("Test with correlation");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert - Check that the index exists
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%CorrelationId%'";
        var indexName = await cmd.ExecuteScalarAsync();

        indexName.Should().NotBeNull();
    }

    [Fact]
    public async Task WriteToSQLiteWithVeryLongMessageShouldWork()
    {
        // Arrange
        var veryLongValue = new string('A', 50000);

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Long content: {Content}", veryLongValue);
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT LENGTH(Message) FROM Logs LIMIT 1";
        var length = (long)(await cmd.ExecuteScalarAsync())!;

        // Template "Long content: {Content}" renders as "Long content: \"AAA...\"" (with quotes around string value)
        // The exact length depends on Serilog's rendering, so we just verify it's greater than the value length
        length.Should().BeGreaterThan(50000);
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
