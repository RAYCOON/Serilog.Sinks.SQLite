// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace Raycoon.Serilog.Sinks.SQLite.Tests;

/// <summary>
/// Tests for JSON-based configuration via Serilog.Settings.Configuration.
/// </summary>
public class JsonConfigurationTests : IDisposable
{
    private readonly string _testDirectory;

    public JsonConfigurationTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), $"SerilogSqliteJsonTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testDirectory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    [Fact]
    public async Task BasicConfiguration_OnlyDatabasePath_CreatesLoggerAndWritesLogs()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "basic.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}"
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Information("Test message from JSON config");
            await Task.Delay(500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue("database file should be created");

        var logCount = await GetLogCountAsync(dbPath, "Logs");
        logCount.Should().Be(1, "one log entry should be written");
    }

    [Fact]
    public async Task FullConfiguration_AllParameters_AppliesAllSettings()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "full.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "MinimumLevel": "Debug",
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}",
                  "tableName": "CustomLogs",
                  "restrictedToMinimumLevel": "Information",
                  "storeTimestampInUtc": true,
                  "autoCreateDatabase": true,
                  "storePropertiesAsJson": true,
                  "storeExceptionDetails": true,
                  "maxMessageLength": 1000,
                  "maxExceptionLength": 5000,
                  "maxPropertiesLength": 2000,
                  "batchSizeLimit": 50,
                  "batchPeriod": "00:00:01",
                  "queueLimit": 5000,
                  "retentionPeriod": "7.00:00:00",
                  "retentionCount": 10000,
                  "maxDatabaseSize": 52428800,
                  "cleanupInterval": "00:30:00",
                  "journalMode": "Wal",
                  "synchronousMode": "Normal",
                  "throwOnError": false
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Debug("This should be filtered out by restrictedToMinimumLevel");
            logger.Information("This should be written");
            logger.Warning("This warning should also be written");
            await Task.Delay(1500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue("database file should be created");

        var logCount = await GetLogCountAsync(dbPath, "CustomLogs");
        logCount.Should().Be(2, "only Information and Warning should be written (Debug filtered)");
    }

    [Fact]
    public async Task TimeSpanParsing_VariousFormats_ParsesCorrectly()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "timespan.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}",
                  "batchPeriod": "00:00:00.500",
                  "retentionPeriod": "30.00:00:00",
                  "cleanupInterval": "02:00:00"
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Information("Testing TimeSpan parsing");
            await Task.Delay(1000);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue("database file should be created");
        var logCount = await GetLogCountAsync(dbPath, "Logs");
        logCount.Should().Be(1);
    }

    [Fact]
    public async Task EnumParsing_JournalModeAndSynchronousMode_ParsesCorrectly()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "enum.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}",
                  "journalMode": "Delete",
                  "synchronousMode": "Full"
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Information("Testing enum parsing");
            await Task.Delay(500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue("database file should be created");

        // Verify journal mode was applied
        await using var connection = new SqliteConnection($"Data Source={dbPath}");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode;";
        var journalMode = (string?)await command.ExecuteScalarAsync();
        journalMode.Should().Be("delete", "journal mode should be Delete");
    }

    [Fact]
    public async Task CustomColumns_ArrayBinding_CreatesColumnsAndIndexes()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "customcolumns.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}",
                  "customColumns": [
                    {
                      "columnName": "UserId",
                      "dataType": "TEXT",
                      "propertyName": "UserId",
                      "allowNull": true,
                      "createIndex": true
                    },
                    {
                      "columnName": "RequestDuration",
                      "dataType": "REAL",
                      "propertyName": "Duration",
                      "allowNull": true,
                      "createIndex": false
                    },
                    {
                      "columnName": "OrderCount",
                      "dataType": "INTEGER",
                      "propertyName": "OrderCount",
                      "allowNull": false,
                      "createIndex": true
                    }
                  ]
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .Enrich.WithProperty("UserId", "user123")
            .Enrich.WithProperty("Duration", 42.5)
            .Enrich.WithProperty("OrderCount", 5)
            .CreateLogger())
        {
            logger.Information("Testing custom columns");
            await Task.Delay(500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue("database file should be created");

        await using var connection = new SqliteConnection($"Data Source={dbPath}");
        await connection.OpenAsync();

        // Verify columns exist
        var columns = await GetTableColumnsAsync(connection, "Logs");
        columns.Should().Contain("UserId");
        columns.Should().Contain("RequestDuration");
        columns.Should().Contain("OrderCount");

        // Verify data was written to custom columns
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT UserId, RequestDuration, OrderCount FROM Logs LIMIT 1";
        await using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();
        reader.GetString(0).Should().Be("user123");
        reader.GetDouble(1).Should().BeApproximately(42.5, 0.001);
        reader.GetInt32(2).Should().Be(5);
    }

    [Fact]
    public async Task DefaultValues_MissingParameters_UsesDefaults()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "defaults.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "MinimumLevel": "Verbose",
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}"
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Verbose("Verbose message");
            logger.Debug("Debug message");
            logger.Information("Info message");
            await Task.Delay(500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue();

        // Default table name should be "Logs"
        var logCount = await GetLogCountAsync(dbPath, "Logs");
        logCount.Should().Be(3, "all levels should be written with Verbose minimum level");

        // Verify WAL mode is the default
        await using var connection = new SqliteConnection($"Data Source={dbPath}");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode;";
        var journalMode = (string?)await command.ExecuteScalarAsync();
        journalMode.Should().Be("wal", "default journal mode should be WAL");
    }

    [Fact]
    public async Task MinimumLevelOverride_CombinedWithSinkLevel_AppliesBothFilters()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "levels.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "MinimumLevel": {
              "Default": "Debug",
              "Override": {
                "Microsoft": "Warning",
                "System": "Error"
              }
            },
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}",
                  "restrictedToMinimumLevel": "Information"
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Debug("This debug should be filtered by sink level");
            logger.Information("This should be written");

            // Simulate logs from Microsoft namespace (would be filtered at Warning)
            logger
                .ForContext("SourceContext", "Microsoft.AspNetCore.Hosting")
                .Information("Microsoft info - filtered by namespace override");

            logger
                .ForContext("SourceContext", "Microsoft.AspNetCore.Hosting")
                .Warning("Microsoft warning - should be written");

            await Task.Delay(500);
        }

        // Assert
        var logCount = await GetLogCountAsync(dbPath, "Logs");
        logCount.Should().Be(2, "Debug filtered by sink, Microsoft.Info filtered by override");
    }

    [Fact]
    public async Task NoCustomColumns_OmittedParameter_NoErrors()
    {
        // Arrange
        // Note: Empty arrays in JSON configuration cause binding issues with Serilog.Settings.Configuration
        // The recommended approach is to simply omit the customColumns parameter when not needed
        var dbPath = Path.Combine(_testDirectory, "no_columns.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}"
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Information("Test without custom columns");
            await Task.Delay(500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue();
        var logCount = await GetLogCountAsync(dbPath, "Logs");
        logCount.Should().Be(1);
    }

    [Fact]
    public async Task NullQueueLimit_UnlimitedQueue()
    {
        // Arrange
        var dbPath = Path.Combine(_testDirectory, "unlimited_queue.db");
        var json = $$"""
        {
          "Serilog": {
            "Using": ["Raycoon.Serilog.Sinks.SQLite"],
            "WriteTo": [
              {
                "Name": "SQLite",
                "Args": {
                  "databasePath": "{{dbPath.Replace("\\", "/", StringComparison.Ordinal)}}",
                  "queueLimit": null
                }
              }
            ]
          }
        }
        """;

        var configuration = BuildConfiguration(json);

        // Act
        await using (var logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger())
        {
            logger.Information("Test with null queue limit");
            await Task.Delay(500);
        }

        // Assert
        File.Exists(dbPath).Should().BeTrue();
        var logCount = await GetLogCountAsync(dbPath, "Logs");
        logCount.Should().Be(1);
    }

    private static IConfiguration BuildConfiguration(string json)
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(stream)
            .Build();
    }

    private static async Task<int> GetLogCountAsync(string dbPath, string tableName)
    {
        await using var connection = new SqliteConnection($"Data Source={dbPath}");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        // Table name is validated internally, not from user input
#pragma warning disable CA2100
        command.CommandText = $"SELECT COUNT(*) FROM [{tableName}]";
#pragma warning restore CA2100
        var result = await command.ExecuteScalarAsync();
        return Convert.ToInt32(result, CultureInfo.InvariantCulture);
    }

    private static async Task<List<string>> GetTableColumnsAsync(SqliteConnection connection, string tableName)
    {
        var columns = new List<string>();
        await using var command = connection.CreateCommand();
        // Table name is validated internally, not from user input
#pragma warning disable CA2100
        command.CommandText = $"PRAGMA table_info([{tableName}])";
#pragma warning restore CA2100
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columns.Add(reader.GetString(1)); // Column name is at index 1
        }

        return columns;
    }
}
