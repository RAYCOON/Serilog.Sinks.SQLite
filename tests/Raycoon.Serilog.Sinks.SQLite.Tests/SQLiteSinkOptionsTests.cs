// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using Raycoon.Serilog.Sinks.SQLite.Options;
using Serilog.Events;

namespace Raycoon.Serilog.Sinks.SQLite.Tests;

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

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ValidateWithInvalidQueueLimitShouldThrow(int queueLimit)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            QueueLimit = queueLimit
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("QueueLimit");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-1000)]
    public void ValidateWithInvalidMaxDatabaseSizeShouldThrow(long maxDatabaseSize)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxDatabaseSize = maxDatabaseSize
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("MaxDatabaseSize");
    }

    [Fact]
    public void ValidateWithNegativeBatchPeriodShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            BatchPeriod = TimeSpan.FromSeconds(-1)
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("BatchPeriod");
    }

    [Fact]
    public void ValidateWithZeroRetentionCountShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            RetentionCount = 0
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("RetentionCount");
    }

    [Fact]
    public void ValidateWithZeroRetentionPeriodShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            RetentionPeriod = TimeSpan.Zero
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("RetentionPeriod");
    }

    [Fact]
    public void AdditionalConnectionParametersShouldBeCloned()
    {
        // Arrange
        var original = new SQLiteSinkOptions
        {
            DatabasePath = "test.db"
        };
        original.AdditionalConnectionParameters["Password"] = "secret";
        original.AdditionalConnectionParameters["Timeout"] = "30";

        // Act
        var clone = original.Clone();

        // Modify original
        original.AdditionalConnectionParameters.Clear();

        // Assert
        clone.AdditionalConnectionParameters.Should().HaveCount(2);
        clone.AdditionalConnectionParameters["Password"].Should().Be("secret");
        clone.AdditionalConnectionParameters["Timeout"].Should().Be("30");
    }

    [Fact]
    public void ValidateWithValidQueueLimitShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            QueueLimit = 5000
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateWithNullQueueLimitShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            QueueLimit = null
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateWithValidMaxDatabaseSizeShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxDatabaseSize = 100 * 1024 * 1024 // 100 MB
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateWithNullMaxDatabaseSizeShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxDatabaseSize = null
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    #region MaxLength Validation

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ValidateWithInvalidMaxMessageLengthShouldThrow(int maxMessageLength)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxMessageLength = maxMessageLength
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("MaxMessageLength");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ValidateWithInvalidMaxExceptionLengthShouldThrow(int maxExceptionLength)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxExceptionLength = maxExceptionLength
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("MaxExceptionLength");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ValidateWithInvalidMaxPropertiesLengthShouldThrow(int maxPropertiesLength)
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxPropertiesLength = maxPropertiesLength
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("MaxPropertiesLength");
    }

    [Fact]
    public void ValidateWithValidMaxLengthsShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxMessageLength = 1000,
            MaxExceptionLength = 2000,
            MaxPropertiesLength = 5000
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void ValidateWithNullMaxLengthsShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            MaxMessageLength = null,
            MaxExceptionLength = null,
            MaxPropertiesLength = null
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    #endregion

    #region CleanupInterval Validation

    [Fact]
    public void ValidateWithZeroCleanupIntervalShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            CleanupInterval = TimeSpan.Zero
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("CleanupInterval");
    }

    [Fact]
    public void ValidateWithNegativeCleanupIntervalShouldThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            CleanupInterval = TimeSpan.FromSeconds(-1)
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("CleanupInterval");
    }

    [Fact]
    public void ValidateWithValidCleanupIntervalShouldNotThrow()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            CleanupInterval = TimeSpan.FromMinutes(5)
        };

        // Act & Assert
        var act = () => options.Validate();
        act.Should().NotThrow();
    }

    #endregion

    #region CustomColumn.Validate

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void CustomColumn_ValidateWithInvalidColumnName_ShouldThrow(string? columnName)
    {
        // Arrange
        var column = new CustomColumn
        {
            ColumnName = columnName!,
            PropertyName = "ValidProp"
        };

        // Act & Assert
        var act = () => column.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("ColumnName");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void CustomColumn_ValidateWithInvalidPropertyName_ShouldThrow(string? propertyName)
    {
        // Arrange
        var column = new CustomColumn
        {
            ColumnName = "ValidCol",
            PropertyName = propertyName!
        };

        // Act & Assert
        var act = () => column.Validate();
        act.Should().Throw<ArgumentException>()
            .WithParameterName("PropertyName");
    }

    [Fact]
    public void CustomColumn_ValidateWithValidValues_ShouldNotThrow()
    {
        // Arrange
        var column = new CustomColumn
        {
            ColumnName = "UserId",
            PropertyName = "UserId",
            DataType = "TEXT",
            AllowNull = true,
            CreateIndex = true
        };

        // Act & Assert
        var act = () => column.Validate();
        act.Should().NotThrow();
    }

    [Fact]
    public void CustomColumn_DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var column = new CustomColumn();

        // Assert
        column.ColumnName.Should().Be(string.Empty);
        column.PropertyName.Should().Be(string.Empty);
        column.DataType.Should().Be("TEXT");
        column.AllowNull.Should().BeTrue();
        column.CreateIndex.Should().BeFalse();
    }

    #endregion

    [Fact]
    public void CloneShouldCopyAllProperties()
    {
        // Arrange
        var original = new SQLiteSinkOptions
        {
            DatabasePath = "test.db",
            TableName = "TestLogs",
            StoreTimestampInUtc = false,
            RestrictedToMinimumLevel = LogEventLevel.Warning,
            RetentionCount = 5000,
            RetentionPeriod = TimeSpan.FromDays(14),
            MaxDatabaseSize = 50 * 1024 * 1024,
            CleanupInterval = TimeSpan.FromMinutes(30),
            BatchSizeLimit = 150,
            BatchPeriod = TimeSpan.FromSeconds(5),
            QueueLimit = 5000,
            AutoCreateDatabase = false,
            StorePropertiesAsJson = false,
            JournalMode = SQLiteJournalMode.Delete,
            SynchronousMode = SQLiteSynchronousMode.Full,
            StoreExceptionDetails = false,
            MaxExceptionLength = 1000,
            MaxMessageLength = 500,
            MaxPropertiesLength = 2000,
            ThrowOnError = true
        };

        // Act
        var clone = original.Clone();

        // Assert
        clone.DatabasePath.Should().Be(original.DatabasePath);
        clone.TableName.Should().Be(original.TableName);
        clone.StoreTimestampInUtc.Should().Be(original.StoreTimestampInUtc);
        clone.RestrictedToMinimumLevel.Should().Be(original.RestrictedToMinimumLevel);
        clone.RetentionCount.Should().Be(original.RetentionCount);
        clone.RetentionPeriod.Should().Be(original.RetentionPeriod);
        clone.MaxDatabaseSize.Should().Be(original.MaxDatabaseSize);
        clone.CleanupInterval.Should().Be(original.CleanupInterval);
        clone.BatchSizeLimit.Should().Be(original.BatchSizeLimit);
        clone.BatchPeriod.Should().Be(original.BatchPeriod);
        clone.QueueLimit.Should().Be(original.QueueLimit);
        clone.AutoCreateDatabase.Should().Be(original.AutoCreateDatabase);
        clone.StorePropertiesAsJson.Should().Be(original.StorePropertiesAsJson);
        clone.JournalMode.Should().Be(original.JournalMode);
        clone.SynchronousMode.Should().Be(original.SynchronousMode);
        clone.StoreExceptionDetails.Should().Be(original.StoreExceptionDetails);
        clone.MaxExceptionLength.Should().Be(original.MaxExceptionLength);
        clone.MaxMessageLength.Should().Be(original.MaxMessageLength);
        clone.MaxPropertiesLength.Should().Be(original.MaxPropertiesLength);
        clone.ThrowOnError.Should().Be(original.ThrowOnError);
    }
}
