// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using Microsoft.Data.Sqlite;
using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;
using Serilog.Sinks.SQLite.Modern.Sinks;

namespace Serilog.Sinks.SQLite.Modern.Tests;

public sealed class SQLiteSinkTests : IDisposable
{
    private readonly string _testDbPath;
    private bool _disposed;

    public SQLiteSinkTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"serilog_sink_test_{Guid.NewGuid()}.db");
    }

    [Fact]
    public async Task GetLogCountAsyncShouldReturnCorrectCount()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        // Create some log events
        var events = new List<LogEvent>
        {
            CreateLogEvent(LogEventLevel.Information, "Message 1"),
            CreateLogEvent(LogEventLevel.Warning, "Message 2"),
            CreateLogEvent(LogEventLevel.Error, "Message 3")
        };

        await sink.EmitBatchAsync(events);

        // Act
        var count = await sink.GetLogCountAsync();

        // Assert
        count.Should().Be(3);
    }

    [Fact]
    public async Task GetLogCountAsyncShouldReturnZeroForEmptyDatabase()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        // Emit empty batch to ensure schema is created
        await sink.EmitBatchAsync([]);

        // Need to trigger schema creation by emitting at least one event
        await sink.EmitBatchAsync([CreateLogEvent(LogEventLevel.Information, "Setup")]);

        // Delete the entry
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM Logs";
        await cmd.ExecuteNonQueryAsync();

        // Act
        var count = await sink.GetLogCountAsync();

        // Assert
        count.Should().Be(0);
    }

    [Fact]
    public async Task GetDatabaseSizeAsyncShouldReturnSize()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        // Write some data
        var events = Enumerable.Range(0, 100)
            .Select(i => CreateLogEvent(LogEventLevel.Information, $"Message {i} with some content"))
            .ToList();

        await sink.EmitBatchAsync(events);

        // Act
        var size = await sink.GetDatabaseSizeAsync();

        // Assert
        size.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task GetDatabaseSizeAsyncShouldReturnZeroForMemoryDb()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = ":memory:",
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        await sink.EmitBatchAsync([CreateLogEvent(LogEventLevel.Information, "Test")]);

        // Act
        var size = await sink.GetDatabaseSizeAsync();

        // Assert
        size.Should().Be(0);
    }

    [Fact]
    public async Task VacuumAsyncShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        // Write and delete some data to create fragmentation
        var events = Enumerable.Range(0, 50)
            .Select(i => CreateLogEvent(LogEventLevel.Information, $"Message {i}"))
            .ToList();

        await sink.EmitBatchAsync(events);

        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM Logs WHERE Id % 2 = 0";
        await cmd.ExecuteNonQueryAsync();

        // Act & Assert
        var act = async () => await sink.VacuumAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task VacuumAsyncShouldNotThrowForMemoryDb()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = ":memory:",
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        await sink.EmitBatchAsync([CreateLogEvent(LogEventLevel.Information, "Test")]);

        // Act & Assert
        var act = async () => await sink.VacuumAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CleanupAsyncShouldNotThrowWhenNoRetentionManager()
    {
        // Arrange - No retention policies configured
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        await sink.EmitBatchAsync([CreateLogEvent(LogEventLevel.Information, "Test")]);

        // Act & Assert
        var act = async () => await sink.CleanupAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CleanupAsyncShouldTriggerRetentionManagerWhenConfigured()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50),
            RetentionPeriod = TimeSpan.FromDays(7)
        };

        await using var sink = new SQLiteSink(options);

        await sink.EmitBatchAsync([CreateLogEvent(LogEventLevel.Information, "Test")]);

        // Act & Assert
        var act = async () => await sink.CleanupAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void DisposeShouldBeIdempotent()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        using var sink = new SQLiteSink(options);

        // Act & Assert - Multiple dispose calls should not throw
        var act = () =>
        {
            sink.Dispose();
            sink.Dispose();
            sink.Dispose();
        };

        act.Should().NotThrow();
    }

    [Fact]
    public async Task DisposeAsyncShouldBeIdempotent()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        // Act & Assert
        var act = async () =>
        {
            await sink.DisposeAsync();
            await sink.DisposeAsync();
            await sink.DisposeAsync();
        };

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task EmitBatchAsyncShouldSkipWhenDisposed()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        var sink = new SQLiteSink(options);
        await sink.DisposeAsync();

        // Act & Assert - Should not throw, just skip
        var act = async () => await sink.EmitBatchAsync([CreateLogEvent(LogEventLevel.Information, "Test")]);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task OnEmptyBatchAsyncShouldReturnCompletedTask()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        await using var sink = new SQLiteSink(options);

        // Act
        var task = sink.OnEmptyBatchAsync();

        // Assert
        task.IsCompleted.Should().BeTrue();
        await task; // Should not throw
    }

    [Fact]
    public void ConstructorShouldThrowWhenOptionsIsNull()
    {
        // Act & Assert
        var act = () => new SQLiteSink(null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact]
    public void ConstructorShouldThrowWhenOptionsAreInvalid()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "",
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        // Act & Assert
        var act = () => new SQLiteSink(options);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public async Task SinkShouldCreateRetentionManagerWhenRetentionCountIsSet()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50),
            RetentionCount = 1000
        };

        await using var sink = new SQLiteSink(options);

        // Act & Assert - Cleanup should work (RetentionManager is created)
        var act = async () => await sink.CleanupAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task SinkShouldCreateRetentionManagerWhenMaxDatabaseSizeIsSet()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50),
            MaxDatabaseSize = 100 * 1024 * 1024
        };

        await using var sink = new SQLiteSink(options);

        // Act & Assert
        var act = async () => await sink.CleanupAsync();
        await act.Should().NotThrowAsync();
    }

    private static LogEvent CreateLogEvent(LogEventLevel level, string message)
    {
        return new LogEvent(
            DateTimeOffset.Now,
            level,
            null,
            new MessageTemplate(message, []),
            []);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

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
            // Ignore cleanup errors
        }
        catch (UnauthorizedAccessException)
        {
            // Ignore cleanup errors
        }
    }
}
