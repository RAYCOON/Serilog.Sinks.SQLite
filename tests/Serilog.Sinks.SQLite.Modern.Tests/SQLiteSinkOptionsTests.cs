// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Tests;

public class SQLiteSinkOptionsTests
{
    [Fact]
    public void DefaultValuesShouldBeCorrect()
    {
        // Arrange & Act
        var options = new SQLiteSinkOptions();

        // Assert
        options.DatabasePath.Should().Be("logs.db");
        options.TableName.Should().Be("Logs");
        options.StoreTimestampInUtc.Should().BeTrue();
        options.RestrictedToMinimumLevel.Should().Be(LogEventLevel.Verbose);
        options.BatchSizeLimit.Should().Be(100);
        options.BatchPeriod.Should().Be(TimeSpan.FromSeconds(2));
        options.QueueLimit.Should().Be(10000);
        options.AutoCreateDatabase.Should().BeTrue();
        options.StorePropertiesAsJson.Should().BeTrue();
        options.JournalMode.Should().Be(SQLiteJournalMode.Wal);
        options.SynchronousMode.Should().Be(SQLiteSynchronousMode.Normal);
        options.StoreExceptionDetails.Should().BeTrue();
        options.ThrowOnError.Should().BeFalse();
        options.RetentionCount.Should().BeNull();
        options.RetentionPeriod.Should().BeNull();
        options.MaxDatabaseSize.Should().BeNull();
    }

    [Fact]
    public void ValidateWithValidOptionsShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            TableName = "Logs",
            BatchSizeLimit = 50,
            BatchPeriod = TimeSpan.FromSeconds(1)
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void ValidateWithInvalidDatabasePathShouldThrow(string? databasePath)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = databasePath!
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("DatabasePath");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void ValidateWithInvalidTableNameShouldThrow(string? tableName)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            TableName = tableName!
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("TableName");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ValidateWithInvalidBatchSizeLimitShouldThrow(int batchSizeLimit)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            BatchSizeLimit = batchSizeLimit
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("BatchSizeLimit");
    }

    [Fact]
    public void ValidateWithZeroBatchPeriodShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            BatchPeriod = TimeSpan.Zero
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("BatchPeriod");
    }

    [Fact]
    public void ValidateWithNegativeRetentionCountShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            RetentionCount = -1
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("RetentionCount");
    }

    [Fact]
    public void ValidateWithNegativeRetentionPeriodShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            RetentionPeriod = TimeSpan.FromDays(-1)
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("RetentionPeriod");
    }

    [Fact]
    public void CloneShouldCreateIndependentCopy()
    {
        // Arrange
        var original = new SQLiteSinkOptions
        {
            DatabasePath = "original.db",
            TableName = "OriginalLogs",
            BatchSizeLimit = 200,
            RetentionPeriod = TimeSpan.FromDays(30),
            CustomColumns = { new CustomColumn { ColumnName = "Test", DataType = "TEXT", PropertyName = "Test" } }
        };

        // Act
        var clone = original.Clone();

        // Modify original
        original.DatabasePath = "modified.db";
        original.TableName = "ModifiedLogs";
        original.CustomColumns.Clear();

        // Assert
        clone.DatabasePath.Should().Be("original.db");
        clone.TableName.Should().Be("OriginalLogs");
        clone.BatchSizeLimit.Should().Be(200);
        clone.RetentionPeriod.Should().Be(TimeSpan.FromDays(30));
        clone.CustomColumns.Should().HaveCount(1);
    }
}
