// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Globalization;
using Microsoft.Data.Sqlite;
using Raycoon.Serilog.Sinks.SQLite.Internal;
using Raycoon.Serilog.Sinks.SQLite.Options;

namespace Raycoon.Serilog.Sinks.SQLite.Tests;

public sealed class DatabaseManagerTests : IDisposable
{
    private readonly string _testDbPath;
    private bool _disposed;

    public DatabaseManagerTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"serilog_dbmanager_test_{Guid.NewGuid()}.db");
    }

    #region PRAGMA Verification

    [Fact]
    public async Task OpenConnectionAsync_SetsJournalModePragma()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            JournalMode = SQLiteJournalMode.Delete
        };
        using var dbManager = new DatabaseManager(options);

        // Act
        await using var connection = await dbManager.OpenConnectionAsync();

        // Assert
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode";
        var result = (string)(await cmd.ExecuteScalarAsync())!;
        result.Should().BeEquivalentTo("delete");
    }

    [Fact]
    public async Task OpenConnectionAsync_SetsSynchronousModePragma()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            SynchronousMode = SQLiteSynchronousMode.Full
        };
        using var dbManager = new DatabaseManager(options);

        // Act
        await using var connection = await dbManager.OpenConnectionAsync();

        // Assert
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA synchronous";
        var result = Convert.ToInt64(await cmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(2); // Full = 2
    }

    [Fact]
    public async Task OpenConnectionAsync_SetsTempStoreMemoryPragma()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act
        await using var connection = await dbManager.OpenConnectionAsync();

        // Assert
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA temp_store";
        var result = Convert.ToInt64(await cmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(2); // MEMORY = 2
    }

    [Fact]
    public async Task OpenConnectionAsync_SetsMmapSizePragma()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act
        await using var connection = await dbManager.OpenConnectionAsync();

        // Assert
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA mmap_size";
        var result = Convert.ToInt64(await cmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(268435456); // 256 MB
    }

    #endregion

    #region Connection String

    [Fact]
    public void BuildConnectionString_WithAdditionalParameters_IncludesInConnectionString()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        options.AdditionalConnectionParameters["Password"] = "secret123";
        using var dbManager = new DatabaseManager(options);

        // Act
        var connection = dbManager.CreateConnection();

        // Assert
        connection.ConnectionString.Should().Contain("Password=secret123");
        connection.Dispose();
    }

    #endregion

    #region Schema

    [Fact]
    public async Task EnsureSchemaAsync_CalledConcurrently_CreatesSchemaOnlyOnce()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act - Call EnsureSchemaAsync 10 times concurrently
        var tasks = Enumerable.Range(0, 10)
            .Select(_ => dbManager.EnsureSchemaAsync())
            .ToArray();

        // Assert - No exceptions should be thrown
        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync();

        // Verify the table exists exactly once
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Logs'";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(1);
    }

    [Fact]
    public async Task EnsureSchemaAsync_WithNestedDirectory_CreatesDirectoryStructure()
    {
        // Arrange
        var deepPath = Path.Combine(Path.GetTempPath(), $"serilog_test_{Guid.NewGuid()}", "sub1", "sub2", "test.db");
        var options = new SQLiteSinkOptions { DatabasePath = deepPath };
        using var dbManager = new DatabaseManager(options);

        try
        {
            // Act
            await dbManager.EnsureSchemaAsync();

            // Assert
            var directory = Path.GetDirectoryName(deepPath)!;
            Directory.Exists(directory).Should().BeTrue();
        }
        finally
        {
            // Cleanup deep directory
            var rootDir = Path.Combine(Path.GetTempPath(), Path.GetFileName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(deepPath)!)!)!));
            DeleteDirectoryIfExists(rootDir);
        }
    }

    [Fact]
    public async Task EnsureSchemaAsync_WithCustomColumnsAllTypes_CreatesCorrectSchema()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "TextCol",
            DataType = "TEXT",
            PropertyName = "TextCol",
            AllowNull = true
        });
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "IntCol",
            DataType = "INTEGER",
            PropertyName = "IntCol",
            AllowNull = false
        });
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "RealCol",
            DataType = "REAL",
            PropertyName = "RealCol",
            AllowNull = true
        });
        using var dbManager = new DatabaseManager(options);

        // Act
        await dbManager.EnsureSchemaAsync();

        // Assert - Query PRAGMA table_info
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA table_info(Logs)";
        await using var reader = await cmd.ExecuteReaderAsync();

        var columns = new Dictionary<string, (string Type, bool NotNull)>();
        while (await reader.ReadAsync())
        {
            var name = reader.GetString(1);
            var type = reader.GetString(2);
            var notNull = reader.GetInt32(3) == 1;
            columns[name] = (type, notNull);
        }

        columns.Should().ContainKey("TextCol");
        columns["TextCol"].Type.Should().Be("TEXT");
        columns["TextCol"].NotNull.Should().BeFalse();

        columns.Should().ContainKey("IntCol");
        columns["IntCol"].Type.Should().Be("INTEGER");
        columns["IntCol"].NotNull.Should().BeTrue();

        columns.Should().ContainKey("RealCol");
        columns["RealCol"].Type.Should().Be("REAL");
        columns["RealCol"].NotNull.Should().BeFalse();
    }

    [Fact]
    public async Task EnsureSchemaAsync_CreatesDefaultIndexes()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act
        await dbManager.EnsureSchemaAsync();

        // Assert
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name";
        await using var reader = await cmd.ExecuteReaderAsync();

        var indexNames = new List<string>();
        while (await reader.ReadAsync())
        {
            indexNames.Add(reader.GetString(0));
        }

        indexNames.Should().Contain("IX_Logs_Timestamp");
        indexNames.Should().Contain("IX_Logs_Level");
        indexNames.Should().Contain("IX_Logs_Timestamp_Level");
    }

    [Fact]
    public async Task EnsureSchemaAsync_WithAutoCreateDatabaseFalse_SkipsSchemaCreation()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            AutoCreateDatabase = false
        };
        using var dbManager = new DatabaseManager(options);

        // Act
        await dbManager.EnsureSchemaAsync();

        // Assert - Database file should not be created
        File.Exists(_testDbPath).Should().BeFalse();
    }

    #endregion

    #region Constructor Validation

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new DatabaseManager(null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    #endregion

    #region CancellationToken

    [Fact]
    public async Task OpenConnectionAsync_WithAlreadyCancelledToken_ThrowsOperationCancelled()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        var act = async () => await dbManager.OpenConnectionAsync(cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task EnsureSchemaAsync_WithAlreadyCancelledToken_ThrowsOperationCancelled()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        var act = async () => await dbManager.EnsureSchemaAsync(cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task GetLogCountAsync_WithAlreadyCancelledToken_ThrowsOperationCancelled()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);
        await dbManager.EnsureSchemaAsync();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        var act = async () => await dbManager.GetLogCountAsync(cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task GetDatabaseSizeAsync_WithAlreadyCancelledToken_ThrowsOperationCancelled()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        var act = async () => await dbManager.GetDatabaseSizeAsync(cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task VacuumAsync_WithAlreadyCancelledToken_ThrowsOperationCancelled()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);
        await dbManager.EnsureSchemaAsync();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        var act = async () => await dbManager.VacuumAsync(cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region EnsureSchemaAsync Edge Cases

    [Fact]
    public async Task EnsureSchemaAsync_CalledTwice_OnlyInitializesOnce()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act - Call twice sequentially
        await dbManager.EnsureSchemaAsync();
        await dbManager.EnsureSchemaAsync(); // Should return immediately

        // Assert - Table should exist
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Logs'";
        var count = (long)(await cmd.ExecuteScalarAsync())!;
        count.Should().Be(1);
    }

    [Fact]
    public async Task EnsureSchemaAsync_WithMemoryDatabase_SkipsDirectoryCreation()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = ":memory:" };
        using var dbManager = new DatabaseManager(options);

        // Act & Assert - Should not throw
        var act = async () => await dbManager.EnsureSchemaAsync();
        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Dispose

    [Fact]
    public void Dispose_CalledMultipleTimes_IsIdempotent()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act & Assert - First explicit Dispose, second via using
        var act = () => dbManager.Dispose();
        act.Should().NotThrow();
    }

    #endregion

    #region Connection String

    [Fact]
    public void CreateConnection_WithMemoryDatabase_SetsMemoryMode()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = ":memory:" };
        using var dbManager = new DatabaseManager(options);

        // Act
        using var connection = dbManager.CreateConnection();

        // Assert
        connection.ConnectionString.Should().Contain("Mode=Memory");
    }

    [Fact]
    public void CreateConnection_ReturnsUnopenedConnection()
    {
        // Arrange
        var options = new SQLiteSinkOptions { DatabasePath = _testDbPath };
        using var dbManager = new DatabaseManager(options);

        // Act
        using var connection = dbManager.CreateConnection();

        // Assert
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    #endregion

    #region Helpers

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

    private static void DeleteDirectoryIfExists(string path)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    #endregion
}
