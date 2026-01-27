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
///   <item><description>Simple configuration with individual parameters</description></item>
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
    /// Writes log events to a SQLite database file using simple parameter configuration.
    /// </summary>
    /// <param name="loggerConfiguration">
    /// The <see cref="LoggerSinkConfiguration"/> being configured. This is typically accessed
    /// via <c>WriteTo</c> on a <see cref="LoggerConfiguration"/> instance.
    /// </param>
    /// <param name="databasePath">
    /// The file path to the SQLite database. Can be a relative or absolute path.
    /// Use <c>:memory:</c> for an in-memory database (not recommended for production).
    /// The directory will be created automatically if it doesn't exist.
    /// </param>
    /// <param name="tableName">
    /// The name of the table where log entries will be stored. Default is <c>"Logs"</c>.
    /// The table will be created automatically if it doesn't exist.
    /// </param>
    /// <param name="restrictedToMinimumLevel">
    /// The minimum <see cref="LogEventLevel"/> for events to be written to SQLite.
    /// Events below this level will be ignored by this sink. Default is <see cref="LogEventLevel.Verbose"/>.
    /// </param>
    /// <param name="storeTimestampInUtc">
    /// When <c>true</c>, timestamps are stored in UTC format. When <c>false</c>, local time is used.
    /// Default is <c>true</c>. UTC is recommended for distributed systems.
    /// </param>
    /// <param name="batchSizeLimit">
    /// The maximum number of events to include in a single batch write operation.
    /// Larger batches improve throughput but increase memory usage. Default is <c>100</c>.
    /// </param>
    /// <param name="batchPeriod">
    /// The time interval between batch write operations. Events are buffered and written
    /// at this interval. Default is <c>2 seconds</c>. Shorter periods reduce data loss risk
    /// but may impact performance.
    /// </param>
    /// <param name="retentionPeriod">
    /// The maximum age of log entries before automatic deletion. Entries older than this
    /// will be removed during cleanup cycles. <c>null</c> means no time-based retention.
    /// </param>
    /// <param name="retentionCount">
    /// The maximum number of log entries to retain. When exceeded, the oldest entries
    /// are deleted. <c>null</c> means no count-based retention limit.
    /// </param>
    /// <returns>
    /// The <see cref="LoggerConfiguration"/> to allow fluent method chaining.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="loggerConfiguration"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="databasePath"/> is <c>null</c>, empty, or whitespace.
    /// </exception>
    /// <example>
    /// Configure with retention policies:
    /// <code>
    /// var logger = new LoggerConfiguration()
    ///     .WriteTo.SQLite(
    ///         databasePath: "logs/app.db",
    ///         tableName: "AppLogs",
    ///         restrictedToMinimumLevel: LogEventLevel.Information,
    ///         retentionPeriod: TimeSpan.FromDays(30),
    ///         retentionCount: 100000)
    ///     .CreateLogger();
    /// </code>
    /// </example>
    public static LoggerConfiguration SQLite(
        this LoggerSinkConfiguration loggerConfiguration,
        string databasePath,
        string tableName = "Logs",
        LogEventLevel restrictedToMinimumLevel = LogEventLevel.Verbose,
        bool storeTimestampInUtc = true,
        int batchSizeLimit = 100,
        TimeSpan? batchPeriod = null,
        TimeSpan? retentionPeriod = null,
        long? retentionCount = null)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        var options = new SQLiteSinkOptions
        {
            DatabasePath = databasePath,
            TableName = tableName,
            RestrictedToMinimumLevel = restrictedToMinimumLevel,
            StoreTimestampInUtc = storeTimestampInUtc,
            BatchSizeLimit = batchSizeLimit,
            BatchPeriod = batchPeriod ?? TimeSpan.FromSeconds(2),
            RetentionPeriod = retentionPeriod,
            RetentionCount = retentionCount
        };

        return loggerConfiguration.Sink(
            SQLiteSinkFactory.Create(options),
            restrictedToMinimumLevel);
    }

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
