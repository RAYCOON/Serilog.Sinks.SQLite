// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using Raycoon.Serilog.Sinks.SQLite.Options;
using Serilog;
using Serilog.Configuration;
using Serilog.Events;

namespace Raycoon.Serilog.Sinks.SQLite.Tests;

public sealed class SQLiteLoggerConfigurationExtensionsTests : IDisposable
{
    private readonly string _testDbPath;
    private bool _disposed;

    public SQLiteLoggerConfigurationExtensionsTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"serilog_ext_test_{Guid.NewGuid()}.db");
    }

    [Fact]
    public void SQLiteWithNullLoggerConfigurationShouldThrow()
    {
        // Arrange
        LoggerSinkConfiguration? config = null;

        // Act & Assert
        var act = () => config!.SQLite(_testDbPath);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("loggerConfiguration");
    }

    [Fact]
    public void SQLiteWithNullDatabasePathShouldThrow()
    {
        // Arrange
        var loggerConfig = new LoggerConfiguration();

        // Act & Assert
        var act = () => loggerConfig.WriteTo.SQLite(null!);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void SQLiteWithEmptyDatabasePathShouldThrow()
    {
        // Arrange
        var loggerConfig = new LoggerConfiguration();

        // Act & Assert
        var act = () => loggerConfig.WriteTo.SQLite("");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void SQLiteWithWhitespaceDatabasePathShouldThrow()
    {
        // Arrange
        var loggerConfig = new LoggerConfiguration();

        // Act & Assert
        var act = () => loggerConfig.WriteTo.SQLite("   ");
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void SQLiteWithConfigureActionNullShouldThrow()
    {
        // Arrange
        var loggerConfig = new LoggerConfiguration();

        // Act & Assert
        var act = () => loggerConfig.WriteTo.SQLite(_testDbPath, (Action<SQLiteSinkOptions>)null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("configure");
    }

    [Fact]
    public void SQLiteWithNullOptionsShouldThrow()
    {
        // Arrange
        var loggerConfig = new LoggerConfiguration();

        // Act & Assert
        var act = () => loggerConfig.WriteTo.SQLite((SQLiteSinkOptions)null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact]
    public void SQLiteWithConfigureActionShouldApplyOptions()
    {
        // Arrange
        var customTableName = "CustomTable";
        var customBatchSize = 200;

        // Act
        using var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.TableName = customTableName;
                options.BatchSizeLimit = customBatchSize;
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
            })
            .CreateLogger();

        // Assert - Logger should be created without throwing
        logger.Should().NotBeNull();
    }

    [Fact]
    public async Task SQLiteWithConfigureActionShouldUseCustomTableName()
    {
        // Arrange
        const string customTableName = "MyCustomLogs";

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.TableName = customTableName;
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
            })
            .CreateLogger())
        {
            logger.Information("Test message");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        // Table name is a compile-time constant, safe from SQL injection
        cmd.CommandText = "SELECT COUNT(*) FROM [MyCustomLogs]";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(1);
    }

    [Fact]
    public void SQLiteShouldReturnLoggerConfigurationForChaining()
    {
        // Arrange & Act
        var result = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50));

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<LoggerConfiguration>();
    }

    [Fact]
    public void SQLiteWithOptionsOverloadShouldReturnLoggerConfigurationForChaining()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            BatchPeriod = TimeSpan.FromMilliseconds(50)
        };

        // Act
        var result = new LoggerConfiguration()
            .WriteTo.SQLite(options);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<LoggerConfiguration>();
    }

    [Fact]
    public void SQLiteWithConfigureOverloadShouldReturnLoggerConfigurationForChaining()
    {
        // Arrange & Act
        var result = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath, options =>
            {
                options.BatchPeriod = TimeSpan.FromMilliseconds(50);
            });

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<LoggerConfiguration>();
    }

    [Fact]
    public void SQLiteShouldAllowFluentChaining()
    {
        // Arrange & Act
        using var logger = new LoggerConfiguration()
            .MinimumLevel.Debug()
            .WriteTo.SQLite(_testDbPath, batchPeriod: TimeSpan.FromMilliseconds(50))
            .Enrich.WithProperty("Application", "Test")
            .CreateLogger();

        // Assert
        logger.Should().NotBeNull();
    }

    [Fact]
    public async Task SQLiteShouldUseProvidedMinimumLevel()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .WriteTo.SQLite(_testDbPath,
                restrictedToMinimumLevel: LogEventLevel.Error,
                batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Debug("Debug message");
            logger.Information("Info message");
            logger.Warning("Warning message");
            logger.Error("Error message");
            logger.Fatal("Fatal message");

            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert - Only Error and Fatal should be stored
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Logs";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(2);
    }

    [Fact]
    public async Task SQLiteShouldUseProvidedRetentionPeriod()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath,
                retentionPeriod: TimeSpan.FromDays(7),
                batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test message");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert - Database should be created
        File.Exists(_testDbPath).Should().BeTrue();
    }

    [Fact]
    public async Task SQLiteShouldUseProvidedRetentionCount()
    {
        // Arrange & Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(_testDbPath,
                retentionCount: 1000,
                batchPeriod: TimeSpan.FromMilliseconds(50))
            .CreateLogger())
        {
            logger.Information("Test message");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert - Database should be created
        File.Exists(_testDbPath).Should().BeTrue();
    }

    [Fact]
    public async Task SQLiteWithOptionsObjectShouldUseProvidedOptions()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            TableName = "OptionsObjectTable",
            BatchPeriod = TimeSpan.FromMilliseconds(50),
            BatchSizeLimit = 50
        };

        // Act
        using (var logger = new LoggerConfiguration()
            .WriteTo.SQLite(options)
            .CreateLogger())
        {
            logger.Information("Test message");
            await Task.Delay(200);
        }

        await Task.Delay(100);

        // Assert
        await using var connection = new SqliteConnection($"Data Source={_testDbPath}");
        await connection.OpenAsync();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM OptionsObjectTable";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(1);
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
