// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Raycoon.Serilog.Sinks.SQLite.Options;
using Serilog.Debugging;
using Serilog.Events;

namespace Raycoon.Serilog.Sinks.SQLite.Internal;

/// <summary>
/// Writes batches of log events efficiently to the SQLite database using transactions.
/// </summary>
/// <remarks>
/// <para>
/// This class is responsible for the actual database write operations and handles:
/// </para>
/// <list type="bullet">
///   <item><description>Building parameterized INSERT statements for safe and efficient writes</description></item>
///   <item><description>Managing database transactions for batch atomicity</description></item>
///   <item><description>Converting log events to database values</description></item>
///   <item><description>Serializing properties to JSON format</description></item>
///   <item><description>Formatting exceptions with full stack traces</description></item>
///   <item><description>Truncating values to configured maximum lengths</description></item>
///   <item><description>Error handling with configurable callbacks and throw behavior</description></item>
/// </list>
/// <para>
/// The writer reuses prepared statements and parameters within a batch for optimal performance.
/// All database operations are performed within a transaction that is committed on success
/// or rolled back on failure.
/// </para>
/// </remarks>
internal sealed class LogEventBatchWriter : IDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly DatabaseManager _databaseManager;
    private readonly string _machineName;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="LogEventBatchWriter"/> class.
    /// </summary>
    /// <param name="options">
    /// The sink configuration options containing truncation limits, column definitions,
    /// and error handling settings. Must not be <c>null</c>.
    /// </param>
    /// <param name="databaseManager">
    /// The database manager for obtaining connections and ensuring schema initialization.
    /// Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="options"/> or <paramref name="databaseManager"/> is <c>null</c>.
    /// </exception>
    public LogEventBatchWriter(SQLiteSinkOptions options, DatabaseManager databaseManager)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _databaseManager = databaseManager ?? throw new ArgumentNullException(nameof(databaseManager));
        _machineName = Environment.MachineName;
    }

    /// <summary>
    /// Writes a batch of log events to the database within a single transaction.
    /// </summary>
    /// <param name="events">The collection of log events to write.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous write operation.</returns>
    /// <remarks>
    /// <para>
    /// This method performs the following operations:
    /// </para>
    /// <list type="number">
    ///   <item><description>Ensures the database schema is initialized</description></item>
    ///   <item><description>Opens a connection and begins a transaction</description></item>
    ///   <item><description>Creates a parameterized INSERT statement</description></item>
    ///   <item><description>Iterates through events, populating parameters and executing the statement</description></item>
    ///   <item><description>Commits the transaction on success or rolls back on failure</description></item>
    /// </list>
    /// <para>
    /// If an error occurs and <see cref="SQLiteSinkOptions.OnError"/> is configured, the callback
    /// is invoked. If <see cref="SQLiteSinkOptions.ThrowOnError"/> is <c>true</c>, the exception
    /// is re-thrown after the callback.
    /// </para>
    /// </remarks>
    public async Task WriteBatchAsync(IEnumerable<LogEvent> events, CancellationToken cancellationToken = default)
    {
        // Materialize to list for count and multiple iteration
        var eventList = events as IReadOnlyCollection<LogEvent> ?? events.ToList();

        if (eventList.Count == 0)
        {
            return;
        }

        try
        {
            await _databaseManager.EnsureSchemaAsync(cancellationToken).ConfigureAwait(false);
            await using var connection = await _databaseManager.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                var insertSql = BuildInsertSql();
                await using var command = connection.CreateCommand();
                command.CommandText = insertSql;
                command.Transaction = (SqliteTransaction)transaction;

                // Create parameters once and reuse them
                var parameters = CreateParameters(command);

                foreach (var logEvent in eventList)
                {
                    PopulateParameters(parameters, logEvent);
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                SelfLog.WriteLine("SQLite batch write completed: {0} events", eventList.Count);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("SQLite batch write failed: {0}", ex);
            _options.OnError?.Invoke(ex);

            if (_options.ThrowOnError)
            {
                throw;
            }
        }
    }

    /// <summary>
    /// Builds the parameterized INSERT SQL statement for the log table.
    /// </summary>
    /// <returns>A complete INSERT SQL statement with parameter placeholders.</returns>
    /// <remarks>
    /// The generated statement includes all standard columns and any custom columns
    /// defined in <see cref="SQLiteSinkOptions.CustomColumns"/>.
    /// </remarks>
    private string BuildInsertSql()
    {
        var columns = new List<string>
        {
            DatabaseManager.Columns.Timestamp,
            DatabaseManager.Columns.Level,
            DatabaseManager.Columns.LevelName,
            DatabaseManager.Columns.Message,
            DatabaseManager.Columns.MessageTemplate,
            DatabaseManager.Columns.Exception,
            DatabaseManager.Columns.Properties,
            DatabaseManager.Columns.SourceContext,
            DatabaseManager.Columns.MachineName,
            DatabaseManager.Columns.ThreadId
        };

        // Custom Columns
        columns.AddRange(_options.CustomColumns.Select(c => c.ColumnName));

        var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
        var parameterList = string.Join(", ", columns.Select(c => $"@{c}"));

        return $"INSERT INTO [{_options.TableName}] ({columnList}) VALUES ({parameterList})";
    }

    /// <summary>
    /// Creates and registers all SQL parameters for the INSERT command.
    /// </summary>
    /// <param name="command">The SQLite command to add parameters to.</param>
    /// <returns>A dictionary mapping column names to their corresponding parameters for efficient value population.</returns>
    /// <remarks>
    /// Parameters are created with appropriate SQLite types for optimal storage and query performance.
    /// The returned dictionary is used by <see cref="PopulateParameters"/> to efficiently set values
    /// for each log event without recreating parameters.
    /// </remarks>
    private Dictionary<string, SqliteParameter> CreateParameters(SqliteCommand command)
    {
        var parameters = new Dictionary<string, SqliteParameter>(StringComparer.OrdinalIgnoreCase);

        var standardParams = new[]
        {
            (DatabaseManager.Columns.Timestamp, SqliteType.Text),
            (DatabaseManager.Columns.Level, SqliteType.Integer),
            (DatabaseManager.Columns.LevelName, SqliteType.Text),
            (DatabaseManager.Columns.Message, SqliteType.Text),
            (DatabaseManager.Columns.MessageTemplate, SqliteType.Text),
            (DatabaseManager.Columns.Exception, SqliteType.Text),
            (DatabaseManager.Columns.Properties, SqliteType.Text),
            (DatabaseManager.Columns.SourceContext, SqliteType.Text),
            (DatabaseManager.Columns.MachineName, SqliteType.Text),
            (DatabaseManager.Columns.ThreadId, SqliteType.Integer)
        };

        foreach (var (name, type) in standardParams)
        {
            var param = command.CreateParameter();
            param.ParameterName = $"@{name}";
            param.SqliteType = type;
            command.Parameters.Add(param);
            parameters[name] = param;
        }

        // Custom Column Parameters
        foreach (var column in _options.CustomColumns)
        {
            var param = command.CreateParameter();
            param.ParameterName = $"@{column.ColumnName}";
            command.Parameters.Add(param);
            parameters[column.ColumnName] = param;
        }

        return parameters;
    }

    /// <summary>
    /// Populates the SQL parameters with values from a log event.
    /// </summary>
    /// <param name="parameters">The dictionary of parameters to populate.</param>
    /// <param name="logEvent">The log event containing the values to extract.</param>
    /// <remarks>
    /// <para>
    /// This method extracts all relevant data from the log event and sets the corresponding
    /// parameter values. It handles:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Timestamp conversion (UTC or local based on configuration)</description></item>
    ///   <item><description>Log level (both numeric and string representations)</description></item>
    ///   <item><description>Message rendering with property substitution</description></item>
    ///   <item><description>Exception formatting with full stack traces</description></item>
    ///   <item><description>Properties serialization to JSON</description></item>
    ///   <item><description>Standard properties (SourceContext, MachineName, ThreadId)</description></item>
    ///   <item><description>Custom column values from log event properties</description></item>
    ///   <item><description>Value truncation based on configured maximum lengths</description></item>
    /// </list>
    /// </remarks>
    private void PopulateParameters(Dictionary<string, SqliteParameter> parameters, LogEvent logEvent)
    {
        // Timestamp
        var timestamp = _options.StoreTimestampInUtc
            ? logEvent.Timestamp.UtcDateTime
            : logEvent.Timestamp.LocalDateTime;
        parameters[DatabaseManager.Columns.Timestamp].Value = timestamp.ToString("O", CultureInfo.InvariantCulture);

        // Level
        parameters[DatabaseManager.Columns.Level].Value = (int)logEvent.Level;
        parameters[DatabaseManager.Columns.LevelName].Value = logEvent.Level.ToString();

        // Message
        var message = logEvent.RenderMessage(CultureInfo.InvariantCulture);
        if (_options.MaxMessageLength.HasValue && message.Length > _options.MaxMessageLength.Value)
        {
            message = message[.._options.MaxMessageLength.Value];
        }
        parameters[DatabaseManager.Columns.Message].Value = message;

        // Message Template
        parameters[DatabaseManager.Columns.MessageTemplate].Value = logEvent.MessageTemplate.Text;

        // Exception
        if (logEvent.Exception != null && _options.StoreExceptionDetails)
        {
            var exception = FormatException(logEvent.Exception);
            if (_options.MaxExceptionLength.HasValue && exception.Length > _options.MaxExceptionLength.Value)
            {
                exception = exception[.._options.MaxExceptionLength.Value];
            }
            parameters[DatabaseManager.Columns.Exception].Value = exception;
        }
        else
        {
            parameters[DatabaseManager.Columns.Exception].Value = DBNull.Value;
        }

        // Properties
        if (_options.StorePropertiesAsJson && logEvent.Properties.Count > 0)
        {
            var properties = FormatPropertiesAsJson(logEvent);
            if (_options.MaxPropertiesLength.HasValue && properties.Length > _options.MaxPropertiesLength.Value)
            {
                properties = properties[.._options.MaxPropertiesLength.Value];
            }
            parameters[DatabaseManager.Columns.Properties].Value = properties;
        }
        else
        {
            parameters[DatabaseManager.Columns.Properties].Value = DBNull.Value;
        }

        // SourceContext
        if (logEvent.Properties.TryGetValue("SourceContext", out var sourceContext))
        {
            parameters[DatabaseManager.Columns.SourceContext].Value = GetScalarValue(sourceContext);
        }
        else
        {
            parameters[DatabaseManager.Columns.SourceContext].Value = DBNull.Value;
        }

        // MachineName
        parameters[DatabaseManager.Columns.MachineName].Value = _machineName;

        // ThreadId
        if (logEvent.Properties.TryGetValue("ThreadId", out var threadId))
        {
            parameters[DatabaseManager.Columns.ThreadId].Value = GetScalarValue(threadId);
        }
        else
        {
            parameters[DatabaseManager.Columns.ThreadId].Value = Environment.CurrentManagedThreadId;
        }

        // Custom Columns
        foreach (var column in _options.CustomColumns)
        {
            if (logEvent.Properties.TryGetValue(column.PropertyName, out var value))
            {
                parameters[column.ColumnName].Value = GetScalarValue(value) ?? DBNull.Value;
            }
            else
            {
                parameters[column.ColumnName].Value = DBNull.Value;
            }
        }
    }

    /// <summary>
    /// Formats an exception and its inner exceptions as a detailed string.
    /// </summary>
    /// <param name="exception">The exception to format.</param>
    /// <returns>A string containing the exception type, message, stack trace, and inner exceptions.</returns>
    private static string FormatException(Exception exception)
    {
        var sb = new StringBuilder();
        FormatExceptionRecursive(exception, sb, 0);
        return sb.ToString();
    }

    /// <summary>
    /// Recursively formats an exception and its inner exceptions.
    /// </summary>
    /// <param name="exception">The exception to format.</param>
    /// <param name="sb">The StringBuilder to append the formatted output to.</param>
    /// <param name="depth">The current recursion depth to prevent infinite loops.</param>
    /// <remarks>
    /// The maximum recursion depth is 10 to prevent stack overflow from circular exception references.
    /// For <see cref="AggregateException"/>, all inner exceptions are formatted individually.
    /// </remarks>
    private static void FormatExceptionRecursive(Exception exception, StringBuilder sb, int depth)
    {
        if (depth > 10) // Prevent infinite recursion
        {
            sb.AppendLine("[Exception depth limit reached]");
            return;
        }

        sb.AppendLine($"{exception.GetType().FullName}: {exception.Message}");

        if (!string.IsNullOrEmpty(exception.StackTrace))
        {
            sb.AppendLine(exception.StackTrace);
        }

        if (exception is AggregateException aggregateException)
        {
            foreach (var innerException in aggregateException.InnerExceptions)
            {
                sb.AppendLine("--- Inner Exception ---");
                FormatExceptionRecursive(innerException, sb, depth + 1);
            }
        }
        else if (exception.InnerException != null)
        {
            sb.AppendLine("--- Inner Exception ---");
            FormatExceptionRecursive(exception.InnerException, sb, depth + 1);
        }
    }

    /// <summary>
    /// Formats log event properties as a JSON object string.
    /// </summary>
    /// <param name="logEvent">The log event containing properties to serialize.</param>
    /// <returns>A JSON string representation of the log event properties.</returns>
    /// <remarks>
    /// Standard properties (SourceContext, ThreadId) are excluded from the JSON output
    /// as they are stored in dedicated columns. The JSON is generated manually for
    /// performance rather than using a full JSON serializer.
    /// </remarks>
    private static string FormatPropertiesAsJson(LogEvent logEvent)
    {
        var sb = new StringBuilder();
        sb.Append('{');

        var first = true;
        foreach (var property in logEvent.Properties)
        {
            if (property.Key is "SourceContext" or "ThreadId")
            {
                continue;
            }

            if (!first)
            {
                sb.Append(',');
            }
            first = false;

            sb.Append('"');
            sb.Append(EscapeJsonString(property.Key));
            sb.Append("\":");
            FormatPropertyValue(property.Value, sb);
        }

        sb.Append('}');
        return sb.ToString();
    }

    /// <summary>
    /// Formats a Serilog property value as JSON, handling all value types.
    /// </summary>
    /// <param name="value">The property value to format.</param>
    /// <param name="sb">The StringBuilder to append the JSON to.</param>
    /// <remarks>
    /// Handles <see cref="ScalarValue"/>, <see cref="SequenceValue"/>,
    /// <see cref="StructureValue"/>, and <see cref="DictionaryValue"/> types.
    /// </remarks>
    private static void FormatPropertyValue(LogEventPropertyValue value, StringBuilder sb)
    {
        switch (value)
        {
            case ScalarValue scalar:
                FormatScalarValue(scalar.Value, sb);
                break;
            case SequenceValue sequence:
                sb.Append('[');
                var firstItem = true;
                foreach (var item in sequence.Elements)
                {
                    if (!firstItem) sb.Append(',');
                    firstItem = false;
                    FormatPropertyValue(item, sb);
                }
                sb.Append(']');
                break;
            case StructureValue structure:
                sb.Append('{');
                var firstProp = true;
                foreach (var prop in structure.Properties)
                {
                    if (!firstProp) sb.Append(',');
                    firstProp = false;
                    sb.Append('"');
                    sb.Append(EscapeJsonString(prop.Name));
                    sb.Append("\":");
                    FormatPropertyValue(prop.Value, sb);
                }
                sb.Append('}');
                break;
            case DictionaryValue dictionary:
                sb.Append('{');
                var firstEntry = true;
                foreach (var entry in dictionary.Elements)
                {
                    if (!firstEntry) sb.Append(',');
                    firstEntry = false;
                    sb.Append('"');
                    sb.Append(EscapeJsonString(entry.Key.Value?.ToString() ?? "null"));
                    sb.Append("\":");
                    FormatPropertyValue(entry.Value, sb);
                }
                sb.Append('}');
                break;
            default:
                sb.Append("null");
                break;
        }
    }

    /// <summary>
    /// Formats a scalar value as JSON with appropriate type handling.
    /// </summary>
    /// <param name="value">The scalar value to format.</param>
    /// <param name="sb">The StringBuilder to append the JSON to.</param>
    /// <remarks>
    /// Handles null, strings, booleans, numeric types, DateTime, DateTimeOffset, Guid,
    /// and falls back to ToString() for unknown types.
    /// </remarks>
    private static void FormatScalarValue(object? value, StringBuilder sb)
    {
        switch (value)
        {
            case null:
                sb.Append("null");
                break;
            case string s:
                sb.Append('"');
                sb.Append(EscapeJsonString(s));
                sb.Append('"');
                break;
            case bool b:
                sb.Append(b ? "true" : "false");
                break;
            case int or long or short or byte or sbyte or uint or ulong or ushort:
                sb.Append(Convert.ToString(value, CultureInfo.InvariantCulture));
                break;
            case float f:
                sb.Append(f.ToString(CultureInfo.InvariantCulture));
                break;
            case double d:
                sb.Append(d.ToString(CultureInfo.InvariantCulture));
                break;
            case decimal dec:
                sb.Append(dec.ToString(CultureInfo.InvariantCulture));
                break;
            case DateTime dt:
                sb.Append('"');
                sb.Append(dt.ToString("O", CultureInfo.InvariantCulture));
                sb.Append('"');
                break;
            case DateTimeOffset dto:
                sb.Append('"');
                sb.Append(dto.ToString("O", CultureInfo.InvariantCulture));
                sb.Append('"');
                break;
            case Guid g:
                sb.Append('"');
                sb.Append(g.ToString());
                sb.Append('"');
                break;
            default:
                sb.Append('"');
                sb.Append(EscapeJsonString(value.ToString() ?? string.Empty));
                sb.Append('"');
                break;
        }
    }

    /// <summary>
    /// Escapes a string for safe inclusion in JSON.
    /// </summary>
    /// <param name="s">The string to escape.</param>
    /// <returns>The escaped string with special characters properly encoded.</returns>
    /// <remarks>
    /// Escapes quotes, backslashes, and control characters as per JSON specification (RFC 8259).
    /// Control characters below space (0x20) are encoded as \uXXXX.
    /// </remarks>
    private static string EscapeJsonString(string s)
    {
        var sb = new StringBuilder(s.Length);
        foreach (var c in s)
        {
            switch (c)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < ' ')
                    {
                        sb.Append("\\u");
                        sb.Append(((int)c).ToString("X4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        sb.Append(c);
                    }
                    break;
            }
        }
        return sb.ToString();
    }

    /// <summary>
    /// Extracts the underlying scalar value from a Serilog property value.
    /// </summary>
    /// <param name="value">The property value to extract from.</param>
    /// <returns>
    /// The underlying value for <see cref="ScalarValue"/> instances, or the string
    /// representation (with quotes trimmed) for other value types.
    /// </returns>
    private static object? GetScalarValue(LogEventPropertyValue value)
    {
        return value switch
        {
            ScalarValue scalar => scalar.Value,
            _ => value.ToString()?.Trim('"')
        };
    }

    /// <summary>
    /// Releases all resources used by the <see cref="LogEventBatchWriter"/>.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
    }
}
