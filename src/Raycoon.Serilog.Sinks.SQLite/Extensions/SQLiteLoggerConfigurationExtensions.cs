// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using Raycoon.Serilog.Sinks.SQLite.Options;
using Raycoon.Serilog.Sinks.SQLite.Sinks;
using Serilog.Configuration;
using Serilog.Events;

namespace Serilog;

/// <summary>
/// Provides extension methods for configuring the SQLite sink in Serilog's fluent configuration API.
/// </summary>
/// <remarks>
/// <para>
/// This class extends <see cref="LoggerSinkConfiguration"/> to provide convenient methods
/// for adding SQLite logging to a Serilog logger configuration. The extensions support
/// multiple configuration patterns:
/// </para>
/// <list type="bullet">
///   <item><description>Simple configuration with individual parameters (supports JSON configuration via <c>Serilog.Settings.Configuration</c>)</description></item>
///   <item><description>Action-based configuration for advanced options</description></item>
///   <item><description>Full options object configuration for complete control</description></item>
/// </list>
/// <para>
/// All extension methods support fluent chaining, allowing them to be combined with
/// other Serilog configuration methods.
/// </para>
/// </remarks>
/// <example>
/// Basic usage with default settings:
/// <code>
/// var logger = new LoggerConfiguration()
///     .WriteTo.SQLite("logs/app.db")
///     .CreateLogger();
/// </code>
/// </example>
public static class SQLiteLoggerConfigurationExtensions
{
    /// <summary>
    /// Writes log events to a SQLite database file using action-based configuration
    /// for advanced options.
    /// </summary>
    /// <param name="loggerConfiguration">
    /// The <see cref="LoggerSinkConfiguration"/> being configured. This is typically accessed
    /// via <c>WriteTo</c> on a <see cref="LoggerConfiguration"/> instance.
    /// </param>
    /// <param name="databasePath">
    /// The file path to the SQLite database. Can be a relative or absolute path.
    /// Use <c>:memory:</c> for an in-memory database (not recommended for production).
    /// </param>
    /// <param name="configure">
    /// An <see cref="Action{T}"/> delegate that configures the <see cref="SQLiteSinkOptions"/>.
    /// This allows access to all configuration options including custom columns, journal modes,
    /// and error handling callbacks.
    /// </param>
    /// <returns>
    /// The <see cref="LoggerConfiguration"/> to allow fluent method chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="loggerConfiguration"/> or <paramref name="configure"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="databasePath"/> is <c>null</c>, empty, or whitespace.
    /// </exception>
    /// <example>
    /// Configure with custom columns and advanced options:
    /// <code>
    /// var logger = new LoggerConfiguration()
    ///     .WriteTo.SQLite("logs/app.db", options =>
    ///     {
    ///         options.TableName = "ApplicationLogs";
    ///         options.RetentionPeriod = TimeSpan.FromDays(30);
    ///         options.MaxDatabaseSize = 100 * 1024 * 1024; // 100 MB
    ///         options.JournalMode = SQLiteJournalMode.Wal;
    ///         options.CustomColumns.Add(new CustomColumn
    ///         {
    ///             ColumnName = "UserId",
    ///             DataType = "TEXT",
    ///             PropertyName = "UserId",
    ///             CreateIndex = true
    ///         });
    ///         options.OnError = ex => Console.WriteLine($"Logging error: {ex.Message}");
    ///     })
    ///     .CreateLogger();
    /// </code>
    /// </example>
    public static LoggerConfiguration SQLite(
        this LoggerSinkConfiguration loggerConfiguration,
        string databasePath,
        Action<SQLiteSinkOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new SQLiteSinkOptions
        {
            DatabasePath = databasePath
        };

        configure(options);

        return loggerConfiguration.Sink(
            SQLiteSinkFactory.Create(options),
            options.RestrictedToMinimumLevel);
    }

    /// <summary>
    /// Writes log events to a SQLite database file with full configuration support for JSON-based configuration
    /// via <c>Serilog.Settings.Configuration</c>.
    /// </summary>
    /// <param name="loggerConfiguration">
    /// The <see cref="LoggerSinkConfiguration"/> being configured. This is typically accessed
    /// via <c>WriteTo</c> on a <see cref="LoggerConfiguration"/> instance.
    /// </param>
    /// <param name="databasePath">
    /// The file path to the SQLite database. Can be a relative or absolute path.
    /// Use <c>:memory:</c> for an in-memory database (not recommended for production).
    /// </param>
    /// <param name="tableName">
    /// The name of the table where log entries will be stored. Default is <c>"Logs"</c>.
    /// </param>
    /// <param name="restrictedToMinimumLevel">
    /// The minimum <see cref="LogEventLevel"/> for events to be written to SQLite.
    /// Default is <see cref="LogEventLevel.Verbose"/>.
    /// </param>
    /// <param name="storeTimestampInUtc">
    /// When <c>true</c>, timestamps are stored in UTC format. Default is <c>true</c>.
    /// </param>
    /// <param name="autoCreateDatabase">
    /// When <c>true</c>, the database and table are created automatically. Default is <c>true</c>.
    /// </param>
    /// <param name="storePropertiesAsJson">
    /// When <c>true</c>, log event properties are stored as JSON. Default is <c>true</c>.
    /// </param>
    /// <param name="storeExceptionDetails">
    /// When <c>true</c>, exception details are stored in the Exception column. Default is <c>true</c>.
    /// </param>
    /// <param name="maxMessageLength">
    /// Maximum length of log messages in characters, or <c>null</c> for unlimited.
    /// </param>
    /// <param name="maxExceptionLength">
    /// Maximum length of exception details in characters, or <c>null</c> for unlimited.
    /// </param>
    /// <param name="maxPropertiesLength">
    /// Maximum length of the JSON properties string in characters, or <c>null</c> for unlimited.
    /// </param>
    /// <param name="batchSizeLimit">
    /// The maximum number of events to include in a single batch write. Default is <c>100</c>.
    /// </param>
    /// <param name="batchPeriod">
    /// The time interval between batch write operations. Default is <c>2 seconds</c>.
    /// Use format <c>"hh:mm:ss"</c> or <c>"d.hh:mm:ss"</c> in JSON configuration.
    /// </param>
    /// <param name="queueLimit">
    /// Maximum number of events in the queue, or <c>null</c> for unlimited. Default is <c>10000</c>.
    /// </param>
    /// <param name="retentionPeriod">
    /// Maximum age of log entries before automatic deletion, or <c>null</c> for unlimited.
    /// Use format <c>"d.hh:mm:ss"</c> in JSON configuration (e.g., <c>"30.00:00:00"</c> for 30 days).
    /// </param>
    /// <param name="retentionCount">
    /// Maximum number of log entries to retain, or <c>null</c> for unlimited.
    /// </param>
    /// <param name="maxDatabaseSize">
    /// Maximum database file size in bytes, or <c>null</c> for unlimited.
    /// </param>
    /// <param name="cleanupInterval">
    /// Interval between cleanup cycles for retention policies. Default is <c>1 hour</c>.
    /// </param>
    /// <param name="journalMode">
    /// The SQLite journal mode. Default is <see cref="SQLiteJournalMode.Wal"/>.
    /// Use string values <c>"Delete"</c>, <c>"Truncate"</c>, <c>"Persist"</c>, <c>"Memory"</c>, <c>"Wal"</c>, or <c>"Off"</c> in JSON.
    /// </param>
    /// <param name="synchronousMode">
    /// The SQLite synchronous mode. Default is <see cref="SQLiteSynchronousMode.Normal"/>.
    /// Use string values <c>"Off"</c>, <c>"Normal"</c>, <c>"Full"</c>, or <c>"Extra"</c> in JSON.
    /// </param>
    /// <param name="throwOnError">
    /// When <c>true</c>, exceptions are thrown on write errors. Default is <c>false</c>.
    /// </param>
    /// <param name="customColumns">
    /// Custom columns to add to the log table for structured queries.
    /// </param>
    /// <returns>
    /// The <see cref="LoggerConfiguration"/> to allow fluent method chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="loggerConfiguration"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="databasePath"/> is <c>null</c>, empty, or whitespace,
    /// or when custom column validation fails.
    /// </exception>
    /// <remarks>
    /// <para>
    /// This overload is specifically designed for JSON-based configuration via <c>Serilog.Settings.Configuration</c>.
    /// All parameters are exposed as flat arguments that can be bound from <c>appsettings.json</c>.
    /// </para>
    /// <para>
    /// <b>Note:</b> The <c>OnError</c> callback and <c>AdditionalConnectionParameters</c> are not available
    /// via JSON configuration as they cannot be serialized. Use programmatic configuration for these features.
    /// </para>
    /// </remarks>
    /// <example>
    /// JSON configuration (appsettings.json):
    /// <code>
    /// {
    ///   "Serilog": {
    ///     "Using": ["Raycoon.Serilog.Sinks.SQLite"],
    ///     "WriteTo": [{
    ///       "Name": "SQLite",
    ///       "Args": {
    ///         "databasePath": "logs/app.db",
    ///         "tableName": "ApplicationLogs",
    ///         "batchSizeLimit": 200,
    ///         "batchPeriod": "00:00:01",
    ///         "retentionPeriod": "30.00:00:00",
    ///         "journalMode": "Wal",
    ///         "customColumns": [
    ///           {
    ///             "columnName": "UserId",
    ///             "dataType": "TEXT",
    ///             "propertyName": "UserId",
    ///             "createIndex": true
    ///           }
    ///         ]
    ///       }
    ///     }]
    ///   }
    /// }
    /// </code>
    /// </example>
    public static LoggerConfiguration SQLite(
        this LoggerSinkConfiguration loggerConfiguration,
        string databasePath,
        string tableName = "Logs",
        LogEventLevel restrictedToMinimumLevel = LogEventLevel.Verbose,
        bool storeTimestampInUtc = true,
        bool autoCreateDatabase = true,
        bool storePropertiesAsJson = true,
        bool storeExceptionDetails = true,
        int? maxMessageLength = null,
        int? maxExceptionLength = null,
        int? maxPropertiesLength = null,
        int batchSizeLimit = 100,
        TimeSpan? batchPeriod = null,
        int? queueLimit = 10000,
        TimeSpan? retentionPeriod = null,
        long? retentionCount = null,
        long? maxDatabaseSize = null,
        TimeSpan? cleanupInterval = null,
        SQLiteJournalMode journalMode = SQLiteJournalMode.Wal,
        SQLiteSynchronousMode synchronousMode = SQLiteSynchronousMode.Normal,
        bool throwOnError = false,
        CustomColumn[]? customColumns = null)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        var options = new SQLiteSinkOptions
        {
            DatabasePath = databasePath,
            TableName = tableName,
            RestrictedToMinimumLevel = restrictedToMinimumLevel,
            StoreTimestampInUtc = storeTimestampInUtc,
            AutoCreateDatabase = autoCreateDatabase,
            StorePropertiesAsJson = storePropertiesAsJson,
            StoreExceptionDetails = storeExceptionDetails,
            MaxMessageLength = maxMessageLength,
            MaxExceptionLength = maxExceptionLength,
            MaxPropertiesLength = maxPropertiesLength,
            BatchSizeLimit = batchSizeLimit,
            BatchPeriod = batchPeriod ?? TimeSpan.FromSeconds(2),
            QueueLimit = queueLimit,
            RetentionPeriod = retentionPeriod,
            RetentionCount = retentionCount,
            MaxDatabaseSize = maxDatabaseSize,
            CleanupInterval = cleanupInterval ?? TimeSpan.FromHours(1),
            JournalMode = journalMode,
            SynchronousMode = synchronousMode,
            ThrowOnError = throwOnError
        };

        if (customColumns is { Length: > 0 })
        {
            foreach (var column in customColumns)
            {
                column.Validate();
                options.CustomColumns.Add(column);
            }
        }

        return loggerConfiguration.Sink(
            SQLiteSinkFactory.Create(options),
            restrictedToMinimumLevel);
    }

    /// <summary>
    /// Writes log events to a SQLite database file using a pre-configured options object.
    /// </summary>
    /// <param name="loggerConfiguration">
    /// The <see cref="LoggerSinkConfiguration"/> being configured. This is typically accessed
    /// via <c>WriteTo</c> on a <see cref="LoggerConfiguration"/> instance.
    /// </param>
    /// <param name="options">
    /// A fully configured <see cref="SQLiteSinkOptions"/> instance. The options object
    /// is cloned internally, so modifications after calling this method won't affect
    /// the sink's behavior.
    /// </param>
    /// <returns>
    /// The <see cref="LoggerConfiguration"/> to allow fluent method chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="loggerConfiguration"/> or <paramref name="options"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the options fail validation (e.g., invalid database path).
    /// </exception>
    /// <example>
    /// Configure using a reusable options object:
    /// <code>
    /// var options = new SQLiteSinkOptions
    /// {
    ///     DatabasePath = "logs/app.db",
    ///     TableName = "Logs",
    ///     RetentionPeriod = TimeSpan.FromDays(7),
    ///     BatchSizeLimit = 200,
    ///     JournalMode = SQLiteJournalMode.Wal
    /// };
    ///
    /// var logger = new LoggerConfiguration()
    ///     .WriteTo.SQLite(options)
    ///     .CreateLogger();
    /// </code>
    /// </example>
    public static LoggerConfiguration SQLite(
        this LoggerSinkConfiguration loggerConfiguration,
        SQLiteSinkOptions options)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(options);

        return loggerConfiguration.Sink(
            SQLiteSinkFactory.Create(options),
            options.RestrictedToMinimumLevel);
    }
}
