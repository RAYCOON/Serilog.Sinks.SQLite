// Copyright (c) 2025- RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using Serilog.Events;

namespace Raycoon.Serilog.Sinks.SQLite.Options;

/// <summary>
/// Configuration options for the SQLite Serilog sink.
/// </summary>
/// <remarks>
/// <para>
/// This class contains all configuration options for customizing the behavior of the SQLite sink.
/// Options are organized into several categories:
/// </para>
/// <list type="bullet">
///   <item><description><b>Database settings</b>: Path, table name, auto-creation</description></item>
///   <item><description><b>Batching</b>: Batch size, period, and queue limits</description></item>
///   <item><description><b>Retention</b>: Time, count, and size-based retention policies</description></item>
///   <item><description><b>Performance</b>: Journal mode, synchronous mode, truncation limits</description></item>
///   <item><description><b>Custom columns</b>: Additional columns mapped from log properties</description></item>
///   <item><description><b>Error handling</b>: Callbacks and throw behavior</description></item>
/// </list>
/// <para>
/// The <see cref="Clone"/> method creates a deep copy of the options, which is used internally
/// to ensure configuration immutability after sink creation.
/// </para>
/// </remarks>
/// <example>
/// Creating options with common settings:
/// <code>
/// var options = new SQLiteSinkOptions
/// {
///     DatabasePath = "logs/app.db",
///     TableName = "ApplicationLogs",
///     RetentionPeriod = TimeSpan.FromDays(30),
///     BatchSizeLimit = 200,
///     JournalMode = SQLiteJournalMode.Wal
/// };
/// </code>
/// </example>
public sealed class SQLiteSinkOptions
{
    private readonly Collection<CustomColumn> _customColumns = [];
    private readonly Dictionary<string, string> _additionalConnectionParameters = [];

    /// <summary>
    /// Gets or sets the path to the SQLite database file.
    /// </summary>
    /// <value>
    /// The database file path. Can be relative, absolute, or <c>:memory:</c> for in-memory databases.
    /// Default is <c>"logs.db"</c>.
    /// </value>
    /// <remarks>
    /// <para>
    /// The path can be:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>A relative path (relative to the application's working directory)</description></item>
    ///   <item><description>An absolute path</description></item>
    ///   <item><description><c>:memory:</c> for an in-memory database (data is lost when the application exits)</description></item>
    /// </list>
    /// <para>
    /// If the directory does not exist and <see cref="AutoCreateDatabase"/> is <c>true</c>,
    /// the directory will be created automatically.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // Relative path
    /// options.DatabasePath = "logs/app.db";
    ///
    /// // Absolute path
    /// options.DatabasePath = "/var/log/myapp/app.db";
    ///
    /// // In-Memory (not recommended for production)
    /// options.DatabasePath = ":memory:";
    /// </code>
    /// </example>
    public string DatabasePath { get; set; } = "logs.db";

    /// <summary>
    /// Gets or sets the name of the table where log entries are stored.
    /// </summary>
    /// <value>
    /// The table name. Default is <c>"Logs"</c>.
    /// </value>
    /// <remarks>
    /// The table is created automatically if it doesn't exist when <see cref="AutoCreateDatabase"/> is <c>true</c>.
    /// </remarks>
    public string TableName { get; set; } = "Logs";

    /// <summary>
    /// Gets or sets whether timestamps should be stored in UTC format.
    /// </summary>
    /// <value>
    /// <c>true</c> to store timestamps in UTC; <c>false</c> to store in local time.
    /// Default is <c>true</c>.
    /// </value>
    /// <remarks>
    /// UTC is recommended for distributed systems and applications deployed across time zones
    /// to ensure consistent timestamp comparison and sorting.
    /// </remarks>
    public bool StoreTimestampInUtc { get; set; } = true;

    /// <summary>
    /// Gets or sets the minimum log level for events to be written to SQLite.
    /// </summary>
    /// <value>
    /// The minimum <see cref="LogEventLevel"/>. Default is <see cref="LogEventLevel.Verbose"/>.
    /// </value>
    /// <remarks>
    /// Events with a level below this threshold will not be written to the database.
    /// This provides sink-level filtering in addition to any global minimum level set on the logger.
    /// </remarks>
    public LogEventLevel RestrictedToMinimumLevel { get; set; } = LogEventLevel.Verbose;

    /// <summary>
    /// Gets or sets the maximum number of log entries to retain in the database.
    /// </summary>
    /// <value>
    /// The maximum entry count, or <c>null</c> for unlimited. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// When the count is exceeded, the oldest entries are automatically deleted during cleanup cycles.
    /// This retention policy is evaluated during the background cleanup at intervals defined by
    /// <see cref="CleanupInterval"/>.
    /// </remarks>
    public long? RetentionCount { get; set; }

    /// <summary>
    /// Gets or sets the maximum age of log entries before automatic deletion.
    /// </summary>
    /// <value>
    /// The retention period, or <c>null</c> for unlimited retention. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// Entries older than this period are automatically deleted during cleanup cycles.
    /// The timestamp comparison uses the format specified by <see cref="StoreTimestampInUtc"/>.
    /// </remarks>
    public TimeSpan? RetentionPeriod { get; set; }

    /// <summary>
    /// Gets or sets the maximum size of the database file in bytes.
    /// </summary>
    /// <value>
    /// The maximum database size in bytes, or <c>null</c> for unlimited. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// <para>
    /// When the size is exceeded, the oldest entries are deleted to reduce the size to approximately
    /// 80% of the maximum. If more than 1000 entries are deleted, a VACUUM operation is performed
    /// to reclaim disk space.
    /// </para>
    /// <para>
    /// Example: <c>100 * 1024 * 1024</c> for 100 MB.
    /// </para>
    /// </remarks>
    public long? MaxDatabaseSize { get; set; }

    /// <summary>
    /// Gets or sets the interval between cleanup cycles for retention policies.
    /// </summary>
    /// <value>
    /// The cleanup interval. Default is <c>1 hour</c>.
    /// </value>
    /// <remarks>
    /// The cleanup runs in the background and evaluates all configured retention policies
    /// (<see cref="RetentionPeriod"/>, <see cref="RetentionCount"/>, <see cref="MaxDatabaseSize"/>).
    /// The first cleanup runs 1 minute after sink initialization to allow application startup.
    /// </remarks>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets the maximum number of log events to include in a single batch write.
    /// </summary>
    /// <value>
    /// The batch size limit. Default is <c>100</c>.
    /// </value>
    /// <remarks>
    /// Larger batches improve write throughput but increase memory usage and latency for
    /// individual events. A batch is written when either this limit is reached or the
    /// <see cref="BatchPeriod"/> expires, whichever comes first.
    /// </remarks>
    public int BatchSizeLimit { get; set; } = 100;

    /// <summary>
    /// Gets or sets the time interval between batch write operations.
    /// </summary>
    /// <value>
    /// The batch period. Default is <c>2 seconds</c>.
    /// </value>
    /// <remarks>
    /// Events are buffered and written at this interval even if <see cref="BatchSizeLimit"/>
    /// hasn't been reached. Shorter periods reduce the risk of data loss but may impact performance.
    /// </remarks>
    public TimeSpan BatchPeriod { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Gets or sets the maximum number of events that can be queued waiting to be written.
    /// </summary>
    /// <value>
    /// The queue limit, or <c>null</c> for unlimited. Default is <c>10000</c>.
    /// </value>
    /// <remarks>
    /// When the queue is full, new events are dropped to prevent memory exhaustion.
    /// This provides back-pressure when the sink cannot keep up with the event rate.
    /// </remarks>
    public int? QueueLimit { get; set; } = 10000;

    /// <summary>
    /// Gets or sets whether the database and table should be created automatically.
    /// </summary>
    /// <value>
    /// <c>true</c> to auto-create; <c>false</c> to require manual creation. Default is <c>true</c>.
    /// </value>
    /// <remarks>
    /// When <c>true</c>, the sink creates the database file, table, and indexes on first write.
    /// Set to <c>false</c> if you need to manage the schema externally.
    /// </remarks>
    public bool AutoCreateDatabase { get; set; } = true;

    /// <summary>
    /// Gets or sets whether log event properties should be stored as JSON.
    /// </summary>
    /// <value>
    /// <c>true</c> to store properties as JSON; <c>false</c> to omit properties. Default is <c>true</c>.
    /// </value>
    /// <remarks>
    /// When <c>true</c>, all log event properties (except SourceContext and ThreadId which have
    /// dedicated columns) are serialized to a JSON string in the Properties column.
    /// </remarks>
    public bool StorePropertiesAsJson { get; set; } = true;

    /// <summary>
    /// Gets or sets the SQLite journal mode for transaction durability.
    /// </summary>
    /// <value>
    /// The journal mode. Default is <see cref="SQLiteJournalMode.Wal"/>.
    /// </value>
    /// <remarks>
    /// <para>
    /// WAL (Write-Ahead Logging) is recommended for most scenarios as it provides:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Better concurrent read/write performance</description></item>
    ///   <item><description>Reduced lock contention</description></item>
    ///   <item><description>Faster writes in most cases</description></item>
    /// </list>
    /// <para>
    /// Other modes may be appropriate for specific scenarios like read-only databases or
    /// network-mounted storage where WAL doesn't work well.
    /// </para>
    /// </remarks>
    public SQLiteJournalMode JournalMode { get; set; } = SQLiteJournalMode.Wal;

    /// <summary>
    /// Gets or sets the SQLite synchronous mode for write durability.
    /// </summary>
    /// <value>
    /// The synchronous mode. Default is <see cref="SQLiteSynchronousMode.Normal"/>.
    /// </value>
    /// <remarks>
    /// <para>
    /// The synchronous mode controls when data is physically written to disk:
    /// </para>
    /// <list type="bullet">
    ///   <item><description><b>Off</b>: Fastest, but data may be lost on system crash</description></item>
    ///   <item><description><b>Normal</b>: Good balance of speed and safety (recommended)</description></item>
    ///   <item><description><b>Full</b>: Safest, but slower due to synchronous disk writes</description></item>
    ///   <item><description><b>Extra</b>: Extra paranoid mode for maximum safety</description></item>
    /// </list>
    /// </remarks>
    public SQLiteSynchronousMode SynchronousMode { get; set; } = SQLiteSynchronousMode.Normal;

    /// <summary>
    /// Gets the additional connection string parameters.
    /// </summary>
    /// <value>
    /// A dictionary of additional connection string parameters.
    /// </value>
    /// <remarks>
    /// Use this to add parameters not directly supported by the options, such as
    /// encryption passwords or custom connection settings.
    /// </remarks>
    public IDictionary<string, string> AdditionalConnectionParameters => _additionalConnectionParameters;

    /// <summary>
    /// Gets or sets a callback that is invoked when a write error occurs.
    /// </summary>
    /// <value>
    /// The error callback, or <c>null</c> for no callback. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// This callback is invoked for any exception during batch writes or cleanup operations.
    /// Use it for logging errors to an alternative destination or for monitoring.
    /// </remarks>
    public Action<Exception>? OnError { get; set; }

    /// <summary>
    /// Gets or sets whether exception details should be stored in the Exception column.
    /// </summary>
    /// <value>
    /// <c>true</c> to store exception details; <c>false</c> to omit them. Default is <c>true</c>.
    /// </value>
    /// <remarks>
    /// Exception details include the exception type, message, stack trace, and inner exceptions.
    /// Set to <c>false</c> to reduce storage space if exception details are not needed.
    /// </remarks>
    public bool StoreExceptionDetails { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum length of exception details in characters.
    /// </summary>
    /// <value>
    /// The maximum length, or <c>null</c> for unlimited. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// When set, exception strings longer than this limit are truncated.
    /// Useful for controlling database size when exceptions contain very long stack traces.
    /// </remarks>
    public int? MaxExceptionLength { get; set; }

    /// <summary>
    /// Gets or sets the maximum length of log messages in characters.
    /// </summary>
    /// <value>
    /// The maximum length, or <c>null</c> for unlimited. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// When set, rendered messages longer than this limit are truncated.
    /// Useful for preventing oversized log entries from consuming excessive storage.
    /// </remarks>
    public int? MaxMessageLength { get; set; }

    /// <summary>
    /// Gets or sets the maximum length of the JSON properties string in characters.
    /// </summary>
    /// <value>
    /// The maximum length, or <c>null</c> for unlimited. Default is <c>null</c>.
    /// </value>
    /// <remarks>
    /// When set, the serialized properties JSON is truncated if it exceeds this length.
    /// Note that truncation may result in invalid JSON.
    /// </remarks>
    public int? MaxPropertiesLength { get; set; }

    /// <summary>
    /// Gets or sets whether exceptions should be thrown on write errors.
    /// </summary>
    /// <value>
    /// <c>true</c> to throw exceptions; <c>false</c> to suppress them. Default is <c>false</c>.
    /// </value>
    /// <remarks>
    /// <para>
    /// By default, errors are silently handled to prevent logging from crashing the application.
    /// Set to <c>true</c> during development to catch configuration or permission issues early.
    /// </para>
    /// <para>
    /// The <see cref="OnError"/> callback is always invoked regardless of this setting.
    /// </para>
    /// </remarks>
    public bool ThrowOnError { get; set; }

    /// <summary>
    /// Gets the collection of custom columns to add to the log table.
    /// </summary>
    /// <value>
    /// A collection of <see cref="CustomColumn"/> definitions.
    /// </value>
    /// <remarks>
    /// <para>
    /// Custom columns allow you to extract specific properties from log events into dedicated
    /// database columns for efficient querying. Each custom column maps a Serilog property
    /// to a table column.
    /// </para>
    /// <para>
    /// Custom columns support:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Any SQLite data type (TEXT, INTEGER, REAL, BLOB)</description></item>
    ///   <item><description>NULL or NOT NULL constraints</description></item>
    ///   <item><description>Automatic index creation for frequently queried columns</description></item>
    /// </list>
    /// </remarks>
    /// <example>
    /// <code>
    /// options.CustomColumns.Add(new CustomColumn
    /// {
    ///     ColumnName = "UserId",
    ///     DataType = "TEXT",
    ///     PropertyName = "UserId",
    ///     CreateIndex = true,
    ///     AllowNull = true
    /// });
    /// </code>
    /// </example>
    public Collection<CustomColumn> CustomColumns => _customColumns;

    /// <summary>
    /// Validates the configuration options and throws an exception if any values are invalid.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when any option has an invalid value, with details in the exception message.
    /// </exception>
    /// <remarks>
    /// This method is called automatically when creating a sink. It validates:
    /// <list type="bullet">
    ///   <item><description>DatabasePath is not null or whitespace</description></item>
    ///   <item><description>TableName is not null or whitespace</description></item>
    ///   <item><description>BatchSizeLimit is greater than 0</description></item>
    ///   <item><description>BatchPeriod is greater than 0</description></item>
    ///   <item><description>QueueLimit (if set) is greater than 0</description></item>
    ///   <item><description>RetentionCount (if set) is greater than 0</description></item>
    ///   <item><description>RetentionPeriod (if set) is greater than 0</description></item>
    ///   <item><description>MaxDatabaseSize (if set) is greater than 0</description></item>
    ///   <item><description>CleanupInterval is greater than 0</description></item>
    ///   <item><description>MaxMessageLength (if set) is greater than 0</description></item>
    ///   <item><description>MaxExceptionLength (if set) is greater than 0</description></item>
    ///   <item><description>MaxPropertiesLength (if set) is greater than 0</description></item>
    /// </list>
    /// </remarks>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(DatabasePath))
        {
            throw new ArgumentException("DatabasePath must not be empty.", nameof(DatabasePath));
        }

        if (string.IsNullOrWhiteSpace(TableName))
        {
            throw new ArgumentException("TableName must not be empty.", nameof(TableName));
        }

        if (BatchSizeLimit <= 0)
        {
            throw new ArgumentException("BatchSizeLimit must be greater than 0.", nameof(BatchSizeLimit));
        }

        if (BatchPeriod <= TimeSpan.Zero)
        {
            throw new ArgumentException("BatchPeriod must be greater than 0.", nameof(BatchPeriod));
        }

        if (QueueLimit.HasValue && QueueLimit.Value <= 0)
        {
            throw new ArgumentException("QueueLimit must be greater than 0.", nameof(QueueLimit));
        }

        if (RetentionCount.HasValue && RetentionCount.Value <= 0)
        {
            throw new ArgumentException("RetentionCount must be greater than 0.", nameof(RetentionCount));
        }

        if (RetentionPeriod.HasValue && RetentionPeriod.Value <= TimeSpan.Zero)
        {
            throw new ArgumentException("RetentionPeriod must be greater than 0.", nameof(RetentionPeriod));
        }

        if (MaxDatabaseSize.HasValue && MaxDatabaseSize.Value <= 0)
        {
            throw new ArgumentException("MaxDatabaseSize must be greater than 0.", nameof(MaxDatabaseSize));
        }

        if (CleanupInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("CleanupInterval must be greater than 0.", nameof(CleanupInterval));
        }

        if (MaxMessageLength.HasValue && MaxMessageLength.Value <= 0)
        {
            throw new ArgumentException("MaxMessageLength must be greater than 0.", nameof(MaxMessageLength));
        }

        if (MaxExceptionLength.HasValue && MaxExceptionLength.Value <= 0)
        {
            throw new ArgumentException("MaxExceptionLength must be greater than 0.", nameof(MaxExceptionLength));
        }

        if (MaxPropertiesLength.HasValue && MaxPropertiesLength.Value <= 0)
        {
            throw new ArgumentException("MaxPropertiesLength must be greater than 0.", nameof(MaxPropertiesLength));
        }
    }

    /// <summary>
    /// Creates a deep copy of the options object.
    /// </summary>
    /// <returns>A new <see cref="SQLiteSinkOptions"/> instance with copied values.</returns>
    /// <remarks>
    /// This method is used internally to ensure the sink's configuration is immutable
    /// after creation. All properties including custom columns and additional parameters
    /// are copied to the new instance.
    /// </remarks>
    public SQLiteSinkOptions Clone()
    {
        var clone = new SQLiteSinkOptions
        {
            DatabasePath = DatabasePath,
            TableName = TableName,
            StoreTimestampInUtc = StoreTimestampInUtc,
            RestrictedToMinimumLevel = RestrictedToMinimumLevel,
            RetentionCount = RetentionCount,
            RetentionPeriod = RetentionPeriod,
            MaxDatabaseSize = MaxDatabaseSize,
            CleanupInterval = CleanupInterval,
            BatchSizeLimit = BatchSizeLimit,
            BatchPeriod = BatchPeriod,
            QueueLimit = QueueLimit,
            AutoCreateDatabase = AutoCreateDatabase,
            StorePropertiesAsJson = StorePropertiesAsJson,
            JournalMode = JournalMode,
            SynchronousMode = SynchronousMode,
            OnError = OnError,
            StoreExceptionDetails = StoreExceptionDetails,
            MaxExceptionLength = MaxExceptionLength,
            MaxMessageLength = MaxMessageLength,
            MaxPropertiesLength = MaxPropertiesLength,
            ThrowOnError = ThrowOnError
        };

        foreach (var param in _additionalConnectionParameters)
        {
            clone._additionalConnectionParameters[param.Key] = param.Value;
        }

        foreach (var column in _customColumns)
        {
            clone._customColumns.Add(column);
        }

        return clone;
    }
}

/// <summary>
/// SQLite journal mode settings that control how transactions are logged.
/// </summary>
/// <remarks>
/// The journal mode affects write performance, concurrency, and data durability.
/// See <see href="https://www.sqlite.org/pragma.html#pragma_journal_mode">SQLite documentation</see> for details.
/// </remarks>
public enum SQLiteJournalMode
{
    /// <summary>
    /// The rollback journal is deleted at the end of each transaction.
    /// </summary>
    /// <remarks>
    /// This is the traditional SQLite journal mode. It works on all file systems
    /// but has lower concurrency than WAL mode.
    /// </remarks>
    Delete,

    /// <summary>
    /// The rollback journal is truncated to zero-length at the end of each transaction.
    /// </summary>
    /// <remarks>
    /// Similar to Delete mode but may be faster on some file systems because
    /// truncation is faster than deletion.
    /// </remarks>
    Truncate,

    /// <summary>
    /// The rollback journal is retained but its header is overwritten to invalidate it.
    /// </summary>
    /// <remarks>
    /// Avoids the overhead of deleting or truncating the journal file.
    /// May be faster but leaves the journal file on disk.
    /// </remarks>
    Persist,

    /// <summary>
    /// The rollback journal is stored entirely in memory.
    /// </summary>
    /// <remarks>
    /// Provides very fast transactions but no durability guarantee for system crashes.
    /// Appropriate for temporary databases or when speed is more important than durability.
    /// </remarks>
    Memory,

    /// <summary>
    /// Write-Ahead Logging mode for better concurrent read/write performance.
    /// </summary>
    /// <remarks>
    /// <para>
    /// WAL mode is recommended for most applications because it:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Allows readers and writers to proceed concurrently</description></item>
    ///   <item><description>Provides faster writes in most scenarios</description></item>
    ///   <item><description>Results in more predictable performance</description></item>
    /// </list>
    /// <para>
    /// WAL requires the -wal and -shm files to be on the same file system as the database.
    /// </para>
    /// </remarks>
    Wal,

    /// <summary>
    /// No rollback journal is maintained. Transactions are not atomic.
    /// </summary>
    /// <remarks>
    /// Only use this mode for read-only databases or when you don't need transaction support.
    /// Data corruption may occur if a crash happens during a write.
    /// </remarks>
    Off
}

/// <summary>
/// SQLite synchronous mode settings that control when data is physically written to disk.
/// </summary>
/// <remarks>
/// The synchronous mode trades off write performance against durability guarantees.
/// See <see href="https://www.sqlite.org/pragma.html#pragma_synchronous">SQLite documentation</see> for details.
/// </remarks>
public enum SQLiteSynchronousMode
{
    /// <summary>
    /// SQLite does not wait for data to reach disk before continuing.
    /// </summary>
    /// <remarks>
    /// Fastest mode but the database might be corrupted if the operating system
    /// crashes or the computer loses power before the data is written to disk.
    /// </remarks>
    Off = 0,

    /// <summary>
    /// SQLite syncs at critical moments but not after every write.
    /// </summary>
    /// <remarks>
    /// Provides a good balance between performance and safety. The database is
    /// protected against corruption from application crashes. There is a small
    /// chance of corruption from an OS crash or power failure.
    /// </remarks>
    Normal = 1,

    /// <summary>
    /// SQLite waits for data to be fully written to disk after each write.
    /// </summary>
    /// <remarks>
    /// The safest mode that protects against corruption from application crashes,
    /// operating system crashes, and power failures. This is the slowest mode.
    /// </remarks>
    Full = 2,

    /// <summary>
    /// Extra-paranoid synchronization for maximum durability.
    /// </summary>
    /// <remarks>
    /// Like Full mode but with extra synchronization calls. Provides additional
    /// protection in certain edge cases at the cost of even slower writes.
    /// </remarks>
    Extra = 3
}

/// <summary>
/// Defines a custom column to be added to the log table.
/// </summary>
/// <remarks>
/// <para>
/// Custom columns allow specific log event properties to be stored in dedicated table columns
/// rather than in the generic JSON Properties column. This enables:
/// </para>
/// <list type="bullet">
///   <item><description>More efficient queries using SQL WHERE clauses</description></item>
///   <item><description>Proper data typing (INTEGER, REAL, TEXT, BLOB)</description></item>
///   <item><description>Index creation for frequently queried properties</description></item>
/// </list>
/// <para>
/// This class supports both programmatic configuration using object initializers and
/// JSON configuration via <c>Serilog.Settings.Configuration</c>.
/// </para>
/// </remarks>
/// <example>
/// Programmatic configuration:
/// <code>
/// var userIdColumn = new CustomColumn
/// {
///     ColumnName = "UserId",
///     DataType = "TEXT",
///     PropertyName = "UserId",
///     CreateIndex = true,
///     AllowNull = true
/// };
///
/// // In logger usage:
/// logger.ForContext("UserId", userId).Information("User logged in");
/// </code>
/// </example>
/// <example>
/// JSON configuration (appsettings.json):
/// <code>
/// {
///   "Serilog": {
///     "WriteTo": [{
///       "Name": "SQLite",
///       "Args": {
///         "databasePath": "logs/app.db",
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
public sealed class CustomColumn
{
    /// <summary>
    /// Gets or sets the name of the column in the database table.
    /// </summary>
    /// <value>
    /// The column name. Must be a valid SQLite identifier.
    /// </value>
    /// <remarks>
    /// The column name should follow SQLite naming conventions and avoid reserved words.
    /// It doesn't need to match the property name.
    /// </remarks>
    public string ColumnName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the SQLite data type for the column.
    /// </summary>
    /// <value>
    /// The SQLite data type (e.g., TEXT, INTEGER, REAL, BLOB). Default is <c>"TEXT"</c>.
    /// </value>
    /// <remarks>
    /// Common SQLite types include:
    /// <list type="bullet">
    ///   <item><description>TEXT - for strings</description></item>
    ///   <item><description>INTEGER - for whole numbers and booleans</description></item>
    ///   <item><description>REAL - for floating-point numbers</description></item>
    ///   <item><description>BLOB - for binary data</description></item>
    /// </list>
    /// </remarks>
    public string DataType { get; set; } = "TEXT";

    /// <summary>
    /// Gets or sets the name of the Serilog property to extract the value from.
    /// </summary>
    /// <value>
    /// The property name as used in log event properties.
    /// </value>
    /// <remarks>
    /// This is the property name used with <c>ForContext</c> or in message templates.
    /// The property must be a scalar value (not an object or array) to be stored correctly.
    /// </remarks>
    public string PropertyName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets whether the column allows NULL values.
    /// </summary>
    /// <value>
    /// <c>true</c> if NULL is allowed; <c>false</c> for NOT NULL constraint.
    /// Default is <c>true</c>.
    /// </value>
    /// <remarks>
    /// Set to <c>false</c> only if every log event is guaranteed to have this property.
    /// Logging events without the property when <c>AllowNull</c> is <c>false</c> will cause errors.
    /// </remarks>
    public bool AllowNull { get; set; } = true;

    /// <summary>
    /// Gets or sets whether an index should be created on this column.
    /// </summary>
    /// <value>
    /// <c>true</c> to create an index; <c>false</c> to skip index creation.
    /// Default is <c>false</c>.
    /// </value>
    /// <remarks>
    /// Create an index for columns that are frequently used in WHERE clauses or JOINs.
    /// Indexes improve query performance but slightly increase write time and storage space.
    /// </remarks>
    public bool CreateIndex { get; set; }

    /// <summary>
    /// Validates the custom column configuration.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// Thrown when <see cref="ColumnName"/> or <see cref="PropertyName"/> is null or whitespace.
    /// </exception>
    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(ColumnName))
        {
            throw new ArgumentException("CustomColumn.ColumnName must not be empty.", nameof(ColumnName));
        }

        if (string.IsNullOrWhiteSpace(PropertyName))
        {
            throw new ArgumentException("CustomColumn.PropertyName must not be empty.", nameof(PropertyName));
        }
    }
}
