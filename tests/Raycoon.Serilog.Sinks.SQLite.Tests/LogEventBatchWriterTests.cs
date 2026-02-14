// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Raycoon.Serilog.Sinks.SQLite.Internal;
using Raycoon.Serilog.Sinks.SQLite.Options;
using Serilog.Events;
using Serilog.Parsing;

namespace Raycoon.Serilog.Sinks.SQLite.Tests;

public sealed class LogEventBatchWriterTests : IDisposable
{
    private readonly string _testDbPath;
    private bool _disposed;

    public LogEventBatchWriterTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"serilog_batchwriter_test_{Guid.NewGuid()}.db");
    }

    #region JSON Serialization - Property Types

    [Fact]
    public async Task WriteBatchAsync_WithSequenceValueProperty_SerializesAsJsonArray()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var sequenceValue = new SequenceValue([
            new ScalarValue(1),
            new ScalarValue(2),
            new ScalarValue(3)
        ]);

        var logEvent = CreateLogEventWithProperty("Numbers", sequenceValue);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var numbers = doc.RootElement.GetProperty("Numbers");
        numbers.ValueKind.Should().Be(JsonValueKind.Array);
        numbers.GetArrayLength().Should().Be(3);
        numbers[0].GetInt32().Should().Be(1);
        numbers[1].GetInt32().Should().Be(2);
        numbers[2].GetInt32().Should().Be(3);
    }

    [Fact]
    public async Task WriteBatchAsync_WithStructureValueProperty_SerializesAsJsonObject()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var structureValue = new StructureValue([
            new LogEventProperty("City", new ScalarValue("Berlin")),
            new LogEventProperty("Country", new ScalarValue("Germany"))
        ]);

        var logEvent = CreateLogEventWithProperty("Address", structureValue);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var address = doc.RootElement.GetProperty("Address");
        address.ValueKind.Should().Be(JsonValueKind.Object);
        address.GetProperty("City").GetString().Should().Be("Berlin");
        address.GetProperty("Country").GetString().Should().Be("Germany");
    }

    [Fact]
    public async Task WriteBatchAsync_WithDictionaryValueProperty_SerializesAsJsonObject()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var dictValue = new DictionaryValue([
            new KeyValuePair<ScalarValue, LogEventPropertyValue>(
                new ScalarValue("key1"), new ScalarValue("value1")),
            new KeyValuePair<ScalarValue, LogEventPropertyValue>(
                new ScalarValue("key2"), new ScalarValue("value2"))
        ]);

        var logEvent = CreateLogEventWithProperty("Dict", dictValue);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var dict = doc.RootElement.GetProperty("Dict");
        dict.ValueKind.Should().Be(JsonValueKind.Object);
        dict.GetProperty("key1").GetString().Should().Be("value1");
        dict.GetProperty("key2").GetString().Should().Be("value2");
    }

    [Fact]
    public async Task WriteBatchAsync_WithNullScalarValue_SerializesAsJsonNull()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperty("NullProp", new ScalarValue(null));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var nullProp = doc.RootElement.GetProperty("NullProp");
        nullProp.ValueKind.Should().Be(JsonValueKind.Null);
    }

    [Fact]
    public async Task WriteBatchAsync_WithBooleanProperty_SerializesAsJsonBoolean()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperties(
            ("IsActive", new ScalarValue(true)),
            ("IsDeleted", new ScalarValue(false)));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        doc.RootElement.GetProperty("IsActive").GetBoolean().Should().BeTrue();
        doc.RootElement.GetProperty("IsDeleted").GetBoolean().Should().BeFalse();
    }

    [Fact]
    public async Task WriteBatchAsync_WithNumericProperties_SerializesCorrectly()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperties(
            ("IntVal", new ScalarValue(42)),
            ("LongVal", new ScalarValue(9876543210L)),
            ("FloatVal", new ScalarValue(3.14f)),
            ("DoubleVal", new ScalarValue(2.71828)),
            ("DecimalVal", new ScalarValue(99.99m)));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        doc.RootElement.GetProperty("IntVal").GetInt32().Should().Be(42);
        doc.RootElement.GetProperty("LongVal").GetInt64().Should().Be(9876543210L);
        doc.RootElement.GetProperty("FloatVal").GetSingle().Should().BeApproximately(3.14f, 0.001f);
        doc.RootElement.GetProperty("DoubleVal").GetDouble().Should().BeApproximately(2.71828, 0.00001);
        doc.RootElement.GetProperty("DecimalVal").GetDecimal().Should().Be(99.99m);
    }

    [Fact]
    public async Task WriteBatchAsync_WithDateTimeProperty_SerializesAsIso8601String()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var dateTime = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var dateTimeOffset = new DateTimeOffset(2025, 6, 15, 12, 30, 0, TimeSpan.FromHours(2));

        var logEvent = CreateLogEventWithProperties(
            ("Dt", new ScalarValue(dateTime)),
            ("Dto", new ScalarValue(dateTimeOffset)));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var dtStr = doc.RootElement.GetProperty("Dt").GetString()!;
        var dtoStr = doc.RootElement.GetProperty("Dto").GetString()!;

        dtStr.Should().Contain("2025-06-15");
        dtoStr.Should().Contain("2025-06-15");
    }

    [Fact]
    public async Task WriteBatchAsync_WithGuidProperty_SerializesAsString()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var guid = Guid.Parse("12345678-1234-1234-1234-123456789012");
        var logEvent = CreateLogEventWithProperty("TraceId", new ScalarValue(guid));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var traceId = doc.RootElement.GetProperty("TraceId").GetString();
        traceId.Should().Be("12345678-1234-1234-1234-123456789012");
    }

    #endregion

    #region Exception Formatting

    [Fact]
    public async Task WriteBatchAsync_WithAggregateException_FormatsAllInnerExceptions()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var aggregateEx = new AggregateException("Multiple errors",
            new InvalidOperationException("Error 1"),
            new ArgumentException("Error 2"),
            new TimeoutException("Error 3"));

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Error,
            aggregateEx,
            new MessageTemplate("Aggregate error", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var exception = await ReadColumnValue<string>(dbManager, "Exception");
        exception.Should().Contain("AggregateException");
        exception.Should().Contain("InvalidOperationException");
        exception.Should().Contain("ArgumentException");
        exception.Should().Contain("TimeoutException");
        exception.Should().Contain("Error 1");
        exception.Should().Contain("Error 2");
        exception.Should().Contain("Error 3");
        // Count the inner exception markers
        var markerCount = exception.Split("--- Inner Exception ---").Length - 1;
        markerCount.Should().Be(3);
    }

    [Fact]
    public async Task WriteBatchAsync_WithDeeplyNestedException_StopsAtDepthLimit()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        // Build a chain of 12 nested exceptions (limit is 10)
        Exception ex = new InvalidOperationException("Deepest");
        for (var i = 11; i >= 0; i--)
        {
            ex = new InvalidOperationException($"Level {i}", ex);
        }

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Error,
            ex,
            new MessageTemplate("Deep exception", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var exception = await ReadColumnValue<string>(dbManager, "Exception");
        exception.Should().Contain("[Exception depth limit reached]");
    }

    #endregion

    #region JSON Escaping

    [Fact]
    public async Task WriteBatchAsync_WithJsonSpecialCharacters_EscapesCorrectly()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var value = "quote:\" backslash:\\ newline:\n return:\r tab:\t backspace:\b formfeed:\f";
        var logEvent = CreateLogEventWithProperty("Special", new ScalarValue(value));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        // The JSON should be valid and parseable
        var doc = JsonDocument.Parse(properties);
        var parsed = doc.RootElement.GetProperty("Special").GetString();
        parsed.Should().Contain("quote:\"");
        parsed.Should().Contain("backslash:\\");
        parsed.Should().Contain("newline:\n");
        parsed.Should().Contain("return:\r");
        parsed.Should().Contain("tab:\t");
    }

    [Fact]
    public async Task WriteBatchAsync_WithControlCharacters_EscapesAsUnicode()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        // \u0001 and \u001F are control characters below space (0x20)
        var value = "ctrl:\u0001end:\u001F";
        var logEvent = CreateLogEventWithProperty("Ctrl", new ScalarValue(value));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        // Verify that the raw JSON contains \u0001 and \u001F escape sequences
        properties.Should().Contain("\\u0001");
        properties.Should().Contain("\\u001F");
        // And it should parse correctly
        var doc = JsonDocument.Parse(properties);
        var parsed = doc.RootElement.GetProperty("Ctrl").GetString();
        parsed.Should().Contain("\u0001");
        parsed.Should().Contain("\u001F");
    }

    #endregion

    #region Custom Columns

    [Fact]
    public async Task WriteBatchAsync_WithCustomColumnIntegerType_StoresValue()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "UserId",
            DataType = "INTEGER",
            PropertyName = "UserId"
        });
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperty("UserId", new ScalarValue(42));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var userId = await ReadColumnValue<long>(dbManager, "UserId");
        userId.Should().Be(42);
    }

    [Fact]
    public async Task WriteBatchAsync_WithCustomColumnRealType_StoresValue()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "Score",
            DataType = "REAL",
            PropertyName = "Score"
        });
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperty("Score", new ScalarValue(3.14));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var score = await ReadColumnValue<double>(dbManager, "Score");
        score.Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public async Task WriteBatchAsync_WithMissingCustomColumnProperty_StoresDbNull()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "OptionalField",
            DataType = "TEXT",
            PropertyName = "OptionalField",
            AllowNull = true
        });
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        // Log event WITHOUT the OptionalField property
        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("No optional field", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT OptionalField FROM Logs LIMIT 1";
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be(DBNull.Value);
    }

    [Fact]
    public async Task WriteBatchAsync_ExcludesSourceContextAndThreadIdFromPropertiesJson()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            [
                new LogEventProperty("SourceContext", new ScalarValue("MyApp.MyClass")),
                new LogEventProperty("ThreadId", new ScalarValue(7)),
                new LogEventProperty("CustomProp", new ScalarValue("kept"))
            ]);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);

        // SourceContext and ThreadId should NOT be in the Properties JSON
        doc.RootElement.TryGetProperty("SourceContext", out var scElement).Should().BeFalse();
        doc.RootElement.TryGetProperty("ThreadId", out var tidElement).Should().BeFalse();

        // But they should be in their dedicated columns
        var sourceContext = await ReadColumnValue<string>(dbManager, "SourceContext");
        sourceContext.Should().Be("MyApp.MyClass");

        // CustomProp should be in the Properties JSON
        doc.RootElement.GetProperty("CustomProp").GetString().Should().Be("kept");
    }

    #endregion

    #region Timestamp

    [Fact]
    public async Task WriteBatchAsync_WithUtcTimestamp_StoresUtcFormat()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.StoreTimestampInUtc = true;
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("UTC test", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var timestamp = await ReadColumnValue<string>(dbManager, "Timestamp");
        // UTC format should end with 'Z' (from .UtcDateTime.ToString("O"))
        timestamp.Should().EndWith("Z");
    }

    [Fact]
    public async Task WriteBatchAsync_WithLocalTimestamp_StoresLocalFormat()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.StoreTimestampInUtc = false;
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.Now,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Local test", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var timestamp = await ReadColumnValue<string>(dbManager, "Timestamp");
        // Local format should contain 'T' but NOT end with 'Z' (unless local is UTC)
        timestamp.Should().Contain("T");
        // It should be a valid ISO 8601 date
        DateTime.TryParse(timestamp, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed)
            .Should().BeTrue();
    }

    #endregion

    #region Truncation Boundary

    [Fact]
    public async Task WriteBatchAsync_WithMessageLengthEqualToMax_DoesNotTruncate()
    {
        // Arrange
        var message = new string('A', 50);
        var options = CreateDefaultOptions();
        options.MaxMessageLength = 50; // Exactly equal to message length
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var parser = new MessageTemplateParser();
        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            parser.Parse(message),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var stored = await ReadColumnValue<string>(dbManager, "Message");
        stored.Should().HaveLength(50);
        stored.Should().Be(message);
    }

    [Fact]
    public async Task WriteBatchAsync_WithMessageLengthExceedingMax_Truncates()
    {
        // Arrange
        var message = new string('A', 51);
        var options = CreateDefaultOptions();
        options.MaxMessageLength = 50;
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var parser = new MessageTemplateParser();
        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            parser.Parse(message),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var stored = await ReadColumnValue<string>(dbManager, "Message");
        stored.Should().HaveLength(50);
    }

    [Fact]
    public async Task WriteBatchAsync_WithExceptionLengthEqualToMax_DoesNotTruncate()
    {
        // Arrange - Create an exception and measure its formatted length
        var options = CreateDefaultOptions();
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var ex = new InvalidOperationException("Test error for boundary check");
        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Error,
            ex,
            new MessageTemplate("Error", []),
            []);

        // First write without max to measure actual length
        await writer.WriteBatchAsync([logEvent]);
        var fullException = await ReadColumnValue<string>(dbManager, "Exception");
        var fullLength = fullException.Length;

        // Now create a new writer with MaxExceptionLength == fullLength
        var options2 = CreateDefaultOptions();
        options2.MaxExceptionLength = fullLength;
        var (dbManager2, writer2) = CreateWriter(options2);
        using var _2 = dbManager2;
        using var __2 = writer2;

        await writer2.WriteBatchAsync([logEvent]);
        var stored = await ReadColumnValue<string>(dbManager2, "Exception");
        stored.Should().HaveLength(fullLength);
        stored.Should().Be(fullException);
    }

    [Fact]
    public async Task WriteBatchAsync_WithMaxPropertiesLengthEqualToActual_DoesNotTruncate()
    {
        // Arrange
        var options = CreateDefaultOptions();
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperty("Key", new ScalarValue("Value"));

        // First write without max to measure actual length
        await writer.WriteBatchAsync([logEvent]);
        var fullProps = await ReadPropertiesJson(dbManager);
        var fullLength = fullProps.Length;

        // Now create a new writer with MaxPropertiesLength == fullLength
        var options2 = CreateDefaultOptions();
        options2.MaxPropertiesLength = fullLength;
        var (dbManager2, writer2) = CreateWriter(options2);
        using var _2 = dbManager2;
        using var __2 = writer2;

        await writer2.WriteBatchAsync([logEvent]);
        var stored = await ReadPropertiesJson(dbManager2);
        stored.Should().HaveLength(fullLength);
        stored.Should().Be(fullProps);
    }

    #endregion

    #region Error Handling

    [Fact]
    public async Task WriteBatchAsync_WithThrowOnErrorTrue_RethrowsException()
    {
        // Arrange - Use an invalid database path that will fail on open
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "/nonexistent/path/to/nowhere/test.db",
            AutoCreateDatabase = false,
            ThrowOnError = true
        };
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []);

        // Act & Assert
        var act = async () => await writer.WriteBatchAsync([logEvent]);
        await act.Should().ThrowAsync<Exception>();
    }

    [Fact]
    public async Task WriteBatchAsync_WithThrowOnErrorFalse_SuppressesException()
    {
        // Arrange
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "/nonexistent/path/to/nowhere/test.db",
            AutoCreateDatabase = false,
            ThrowOnError = false
        };
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []);

        // Act & Assert
        var act = async () => await writer.WriteBatchAsync([logEvent]);
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task WriteBatchAsync_OnError_InvokesOnErrorCallback()
    {
        // Arrange
        Exception? capturedEx = null;
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "/nonexistent/path/to/nowhere/test.db",
            AutoCreateDatabase = false,
            ThrowOnError = false,
            OnError = ex => capturedEx = ex
        };
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        capturedEx.Should().NotBeNull();
    }

    [Fact]
    public async Task WriteBatchAsync_OnError_InvokesCallbackAndThenThrowsWhenThrowOnErrorTrue()
    {
        // Arrange
        Exception? capturedEx = null;
        var options = new SQLiteSinkOptions
        {
            DatabasePath = "/nonexistent/path/to/nowhere/test.db",
            AutoCreateDatabase = false,
            ThrowOnError = true,
            OnError = ex => capturedEx = ex
        };
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []);

        // Act & Assert
        var act = async () => await writer.WriteBatchAsync([logEvent]);
        await act.Should().ThrowAsync<Exception>();
        capturedEx.Should().NotBeNull();
    }

    [Fact]
    public async Task WriteBatchAsync_WithEmptyBatch_ReturnsWithoutWriting()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        // Act
        await writer.WriteBatchAsync([]);

        // Assert - Database should not be created (schema not initialized)
        File.Exists(_testDbPath).Should().BeFalse();
    }

    [Fact]
    public async Task WriteBatchAsync_WithStorePropertiesAsJsonFalse_StoresDbNull()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.StorePropertiesAsJson = false;
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = CreateLogEventWithProperty("SomeProp", new ScalarValue("value"));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Properties FROM Logs LIMIT 1";
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be(DBNull.Value);
    }

    [Fact]
    public async Task WriteBatchAsync_WithStoreExceptionDetailsFalse_StoresDbNull()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.StoreExceptionDetails = false;
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Error,
            new InvalidOperationException("Test error"),
            new MessageTemplate("Error occurred", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT Exception FROM Logs LIMIT 1";
        var result = await cmd.ExecuteScalarAsync();
        result.Should().Be(DBNull.Value);
    }

    #endregion

    #region CancellationToken

    [Fact]
    public async Task WriteBatchAsync_WithAlreadyCancelledToken_ThrowsOrSuppresses()
    {
        // Arrange
        var options = CreateDefaultOptions();
        options.ThrowOnError = true;
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        var act = async () => await writer.WriteBatchAsync([logEvent], cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    #endregion

    #region Constructor Validation

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        var options = CreateDefaultOptions();
        using var dbManager = new DatabaseManager(options);

        // Act & Assert
        var act = () => new LogEventBatchWriter(null!, dbManager);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("options");
    }

    [Fact]
    public void Constructor_WithNullDatabaseManager_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new LogEventBatchWriter(CreateDefaultOptions(), null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("databaseManager");
    }

    #endregion

    #region JSON Serialization - Custom Object Types

    [Fact]
    public async Task WriteBatchAsync_WithCustomObjectScalarValue_UsesToStringFallback()
    {
        // Arrange - Uri is not a standard scalar type, triggers default ToString() branch
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var uri = new Uri("https://example.com/path");
        var logEvent = CreateLogEventWithProperty("Endpoint", new ScalarValue(uri));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var endpoint = doc.RootElement.GetProperty("Endpoint").GetString();
        endpoint.Should().Be("https://example.com/path");
    }

    [Fact]
    public async Task WriteBatchAsync_WithSequenceValueInCustomColumn_UsesToStringFallback()
    {
        // Arrange - Custom column with a non-scalar value falls back to GetScalarValue → ToString().Trim('"')
        var options = CreateDefaultOptions();
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "Tags",
            DataType = "TEXT",
            PropertyName = "Tags"
        });
        var (dbManager, writer) = CreateWriter(options);
        using var _ = dbManager;
        using var __ = writer;

        var sequence = new SequenceValue([
            new ScalarValue("tag1"),
            new ScalarValue("tag2")
        ]);
        var logEvent = CreateLogEventWithProperty("Tags", sequence);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert - GetScalarValue for non-ScalarValue returns ToString().Trim('"')
        var tags = await ReadColumnValue<string>(dbManager, "Tags");
        tags.Should().NotBeNullOrEmpty();
        tags.Should().Contain("tag1");
        tags.Should().Contain("tag2");
    }

    #endregion

    #region JSON Escaping - All Control Characters

    [Fact]
    public async Task WriteBatchAsync_WithAllControlCharacters_EscapesAllCorrectly()
    {
        // Arrange - Build a string with all control characters \u0000-\u001F
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        // Characters that have dedicated escapes: \b(0x08), \t(0x09), \n(0x0A), \f(0x0C), \r(0x0D)
        // All others should be escaped as \uXXXX
        var controlChars = new string(Enumerable.Range(0, 32).Select(i => (char)i).ToArray());
        var logEvent = CreateLogEventWithProperty("Ctrl", new ScalarValue(controlChars));

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert - The JSON should be parseable
        var properties = await ReadPropertiesJson(dbManager);
        var doc = JsonDocument.Parse(properties);
        var parsed = doc.RootElement.GetProperty("Ctrl").GetString();

        // Verify round-trip: all control characters should survive JSON encode/decode
        parsed.Should().HaveLength(32);
        for (var i = 0; i < 32; i++)
        {
            parsed![i].Should().Be((char)i, $"control character at position {i} (0x{i:X2}) should survive round-trip");
        }

        // Verify raw JSON contains expected escape patterns
        properties.Should().Contain("\\u0000"); // NUL
        properties.Should().Contain("\\u0001"); // SOH
        properties.Should().Contain("\\b");     // BS (0x08)
        properties.Should().Contain("\\t");     // TAB (0x09)
        properties.Should().Contain("\\n");     // LF (0x0A)
        properties.Should().Contain("\\f");     // FF (0x0C)
        properties.Should().Contain("\\r");     // CR (0x0D)
        properties.Should().Contain("\\u000E"); // SO
        properties.Should().Contain("\\u001F"); // US
    }

    #endregion

    #region Dispose Idempotency

    [Fact]
    public void Dispose_CalledMultipleTimes_IsIdempotent()
    {
        // Arrange
        var options = CreateDefaultOptions();
        using var dbManager = new DatabaseManager(options);
        using var writer = new LogEventBatchWriter(options, dbManager);

        // Act & Assert - First explicit Dispose, second via using
        var act = () => writer.Dispose();
        act.Should().NotThrow();
    }

    #endregion

    #region ThreadId Source

    [Fact]
    public async Task WriteBatchAsync_WithThreadIdProperty_UsesPropertyValue()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            [new LogEventProperty("ThreadId", new ScalarValue(42))]);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var threadId = await ReadColumnValue<long>(dbManager, "ThreadId");
        threadId.Should().Be(42);
    }

    [Fact]
    public async Task WriteBatchAsync_WithoutThreadIdProperty_UsesEnvironmentThreadId()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []); // No ThreadId property

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert - Should be a valid thread ID (> 0)
        var threadId = await ReadColumnValue<long>(dbManager, "ThreadId");
        threadId.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task WriteBatchAsync_StoresMessageTemplate()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplateParser().Parse("Hello {Name}"),
            [new LogEventProperty("Name", new ScalarValue("World"))]);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var template = await ReadColumnValue<string>(dbManager, "MessageTemplate");
        template.Should().Be("Hello {Name}");
    }

    [Fact]
    public async Task WriteBatchAsync_StoresMachineName()
    {
        // Arrange
        var (dbManager, writer) = CreateWriterWithDefaults();
        using var _ = dbManager;
        using var __ = writer;

        var logEvent = new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            []);

        // Act
        await writer.WriteBatchAsync([logEvent]);

        // Assert
        var machineName = await ReadColumnValue<string>(dbManager, "MachineName");
        machineName.Should().Be(Environment.MachineName);
    }

    #endregion

    #region Helpers

    private SQLiteSinkOptions CreateDefaultOptions()
    {
        return new SQLiteSinkOptions
        {
            DatabasePath = _testDbPath,
            StoreTimestampInUtc = true
        };
    }

    private (DatabaseManager dbManager, LogEventBatchWriter writer) CreateWriterWithDefaults()
    {
        return CreateWriter(CreateDefaultOptions());
    }

    private static (DatabaseManager dbManager, LogEventBatchWriter writer) CreateWriter(SQLiteSinkOptions options)
    {
        var dbManager = new DatabaseManager(options);
        var writer = new LogEventBatchWriter(options, dbManager);
        return (dbManager, writer);
    }

    private static LogEvent CreateLogEventWithProperty(string name, LogEventPropertyValue value)
    {
        return new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            [new LogEventProperty(name, value)]);
    }

    private static LogEvent CreateLogEventWithProperties(params (string Name, LogEventPropertyValue Value)[] properties)
    {
        var logProps = properties.Select(p => new LogEventProperty(p.Name, p.Value)).ToArray();
        return new LogEvent(
            DateTimeOffset.UtcNow,
            LogEventLevel.Information,
            null,
            new MessageTemplate("Test", []),
            logProps);
    }

    private static async Task<string> ReadPropertiesJson(DatabaseManager dbManager)
    {
        return await ReadColumnValue<string>(dbManager, "Properties");
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security",
        "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "Column names come from test constants, not user input")]
    private static async Task<T> ReadColumnValue<T>(DatabaseManager dbManager, string columnName)
    {
        await using var conn = dbManager.CreateConnection();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT [{columnName}] FROM Logs ORDER BY Id DESC LIMIT 1";
        var result = await cmd.ExecuteScalarAsync();
        return (T)result!;
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

    #endregion
}
