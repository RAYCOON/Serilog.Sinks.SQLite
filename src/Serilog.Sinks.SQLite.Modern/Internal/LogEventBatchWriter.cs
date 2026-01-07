// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Internal;

/// <summary>
/// Schreibt Log-Events effizient in Batches in die SQLite-Datenbank.
/// </summary>
internal sealed class LogEventBatchWriter : IDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly DatabaseManager _databaseManager;
    private readonly string _machineName;
    private bool _disposed;

    public LogEventBatchWriter(SQLiteSinkOptions options, DatabaseManager databaseManager)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _databaseManager = databaseManager ?? throw new ArgumentNullException(nameof(databaseManager));
        _machineName = Environment.MachineName;
    }

    /// <summary>
    /// Schreibt einen Batch von Log-Events in die Datenbank.
    /// </summary>
    public async Task WriteBatchAsync(IEnumerable<LogEvent> events, CancellationToken cancellationToken = default)
    {
        // Materialisiere zu Liste für Count und mehrfache Iteration
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

                // Parameter einmal erstellen und wiederverwenden
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
    /// Baut das INSERT-Statement.
    /// </summary>
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
    /// Erstellt die Parameter für das Command.
    /// </summary>
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
    /// Füllt die Parameter mit Werten aus dem LogEvent.
    /// </summary>
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
    /// Formatiert eine Exception als String.
    /// </summary>
    private static string FormatException(Exception exception)
    {
        var sb = new StringBuilder();
        FormatExceptionRecursive(exception, sb, 0);
        return sb.ToString();
    }

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
    /// Extrahiert einen skalaren Wert aus einer LogEventPropertyValue.
    /// </summary>
    private static object? GetScalarValue(LogEventPropertyValue value)
    {
        return value switch
        {
            ScalarValue scalar => scalar.Value,
            _ => value.ToString()?.Trim('"')
        };
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
    }
}
