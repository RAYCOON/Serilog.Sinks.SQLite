// Copyright (c) 2025 RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;
using Serilog.Sinks.SQLite.Modern.Internal;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Sinks;

/// <summary>
/// A Serilog sink that writes log events to a SQLite database.
/// </summary>
/// <remarks>
/// <para>
/// This sink provides high-performance, asynchronous logging to SQLite databases with the following features:
/// </para>
/// <list type="bullet">
///   <item><description>Batched writes for optimal throughput using transactions</description></item>
///   <item><description>Automatic schema creation with customizable table structure</description></item>
///   <item><description>Multiple retention policies (time-based, count-based, size-based)</description></item>
///   <item><description>Custom columns for efficient querying of specific properties</description></item>
///   <item><description>Configurable WAL mode and synchronous settings for performance tuning</description></item>
///   <item><description>Support for both file-based and in-memory databases</description></item>
/// </list>
/// <para>
/// The sink implements <see cref="IBatchedLogEventSink"/> and is typically wrapped by
/// <see cref="PeriodicBatchingSink"/> for automatic batching. Use <see cref="SQLiteSinkFactory.Create"/>
/// to create a properly configured sink instance.
/// </para>
/// </remarks>
/// <example>
/// Using the sink via extension methods:
/// <code>
/// var logger = new LoggerConfiguration()
///     .WriteTo.SQLite("logs/app.db", options =>
///     {
///         options.TableName = "ApplicationLogs";
///         options.RetentionPeriod = TimeSpan.FromDays(30);
///         options.CustomColumns.Add(new CustomColumn
///         {
///             ColumnName = "UserId",
///             DataType = "TEXT",
///             PropertyName = "UserId",
///             CreateIndex = true
///         });
///     })
///     .CreateLogger();
/// </code>
/// </example>
public sealed class SQLiteSink : Serilog.Sinks.PeriodicBatching.IBatchedLogEventSink, IDisposable, IAsyncDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly DatabaseManager _databaseManager;
    private readonly LogEventBatchWriter _batchWriter;
    private readonly RetentionManager? _retentionManager;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="SQLiteSink"/> class.
    /// </summary>
    /// <param name="options">
    /// The configuration options for the sink. The options are cloned internally to ensure
    /// immutability after construction. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="options"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the options fail validation (e.g., invalid database path or batch settings).
    /// </exception>
    /// <remarks>
    /// <para>
    /// The constructor performs the following initialization:
    /// </para>
    /// <list type="number">
    ///   <item><description>Clones and validates the provided options</description></item>
    ///   <item><description>Creates the <see cref="DatabaseManager"/> for connection handling</description></item>
    ///   <item><description>Creates the <see cref="LogEventBatchWriter"/> for batch writes</description></item>
    ///   <item><description>Creates the <see cref="RetentionManager"/> if retention policies are configured</description></item>
    /// </list>
    /// <para>
    /// Note that the database schema is not created until the first write operation.
    /// </para>
    /// </remarks>
    public SQLiteSink(SQLiteSinkOptions options)
    {
        _options = options?.Clone() ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();

        _databaseManager = new DatabaseManager(_options);
        _batchWriter = new LogEventBatchWriter(_options, _databaseManager);

        // Create retention manager only if retention is configured
        // The manager starts automatically in the constructor
        if (_options.RetentionPeriod.HasValue ||
            _options.RetentionCount.HasValue ||
            _options.MaxDatabaseSize.HasValue)
        {
            _retentionManager = new RetentionManager(_options, _databaseManager);
        }

        SelfLog.WriteLine("SQLite sink initialized: {0}", _options.DatabasePath);
    }

    /// <summary>
    /// Writes a batch of log events to the SQLite database asynchronously.
    /// </summary>
    /// <param name="batch">The collection of log events to write.</param>
    /// <returns>A task representing the asynchronous write operation.</returns>
    /// <remarks>
    /// <para>
    /// This method is called by the <see cref="PeriodicBatchingSink"/> wrapper when
    /// either the batch size limit is reached or the batch period expires.
    /// </para>
    /// <para>
    /// If the sink has been disposed, this method returns immediately without writing.
    /// Errors during writing are handled according to the <see cref="SQLiteSinkOptions.ThrowOnError"/>
    /// and <see cref="SQLiteSinkOptions.OnError"/> settings.
    /// </para>
    /// </remarks>
    public async Task EmitBatchAsync(IEnumerable<LogEvent> batch)
    {
        if (_disposed)
        {
            return;
        }

        await _batchWriter.WriteBatchAsync(batch).ConfigureAwait(false);
    }

    /// <summary>
    /// Called when no events are available to write during a batch period.
    /// </summary>
    /// <returns>A completed task.</returns>
    /// <remarks>
    /// This implementation does nothing as there is no work to do when the batch is empty.
    /// </remarks>
    public Task OnEmptyBatchAsync()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the current number of log entries stored in the database.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the number of log entries in the configured table.
    /// </returns>
    /// <remarks>
    /// This method is useful for monitoring the log count or implementing custom retention logic.
    /// </remarks>
    public async Task<long> GetLogCountAsync(CancellationToken cancellationToken = default)
    {
        return await _databaseManager.GetLogCountAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the current size of the database file in bytes.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the database size in bytes, or <c>0</c> for in-memory databases.
    /// </returns>
    /// <remarks>
    /// This method is useful for monitoring database growth or implementing custom size-based retention.
    /// </remarks>
    public async Task<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default)
    {
        return await _databaseManager.GetDatabaseSizeAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Triggers an immediate cleanup of log entries based on configured retention policies.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous cleanup operation.</returns>
    /// <remarks>
    /// <para>
    /// This method triggers the same cleanup logic that runs automatically in the background,
    /// but executes it immediately. It evaluates all configured retention policies:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Time-based: Deletes entries older than <see cref="SQLiteSinkOptions.RetentionPeriod"/></description></item>
    ///   <item><description>Count-based: Keeps only the newest <see cref="SQLiteSinkOptions.RetentionCount"/> entries</description></item>
    ///   <item><description>Size-based: Reduces database to 80% of <see cref="SQLiteSinkOptions.MaxDatabaseSize"/></description></item>
    /// </list>
    /// <para>
    /// If no retention policies are configured (no <see cref="RetentionManager"/> was created),
    /// this method returns immediately without doing anything.
    /// </para>
    /// </remarks>
    public async Task CleanupAsync(CancellationToken cancellationToken = default)
    {
        if (_retentionManager != null)
        {
            await _retentionManager.CleanupNowAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes the SQLite VACUUM command to optimize the database file.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous VACUUM operation.</returns>
    /// <remarks>
    /// <para>
    /// VACUUM rebuilds the database file, repacking it into a minimal amount of disk space.
    /// This is useful after deleting large amounts of data to reclaim disk space.
    /// </para>
    /// <para>
    /// Note that VACUUM:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>Requires exclusive access to the database</description></item>
    ///   <item><description>May take significant time for large databases</description></item>
    ///   <item><description>Temporarily requires up to double the database size in free disk space</description></item>
    ///   <item><description>Is a no-op for in-memory databases</description></item>
    /// </list>
    /// </remarks>
    public async Task VacuumAsync(CancellationToken cancellationToken = default)
    {
        await _databaseManager.VacuumAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases all resources used by the <see cref="SQLiteSink"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This method performs the following cleanup in order:
    /// </para>
    /// <list type="number">
    ///   <item><description>Stops the retention manager's background cleanup task (if active)</description></item>
    ///   <item><description>Disposes the batch writer</description></item>
    ///   <item><description>Disposes the database manager</description></item>
    /// </list>
    /// <para>
    /// This method is idempotent and can be called multiple times safely.
    /// </para>
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        _retentionManager?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _batchWriter.Dispose();
        _databaseManager.Dispose();

        SelfLog.WriteLine("SQLite sink disposed: {0}", _options.DatabasePath);
    }

    /// <summary>
    /// Asynchronously releases all resources used by the <see cref="SQLiteSink"/>.
    /// </summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    /// <remarks>
    /// <para>
    /// This method performs the same cleanup as <see cref="Dispose"/> but asynchronously
    /// waits for the retention manager's background task to complete. Prefer this method
    /// when disposing in an async context.
    /// </para>
    /// <para>
    /// This method is idempotent and can be called multiple times safely.
    /// </para>
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_retentionManager != null)
        {
            await _retentionManager.DisposeAsync().ConfigureAwait(false);
        }

        _batchWriter.Dispose();
        _databaseManager.Dispose();

        SelfLog.WriteLine("SQLite sink disposed: {0}", _options.DatabasePath);
    }
}

/// <summary>
/// Factory for creating <see cref="SQLiteSink"/> instances wrapped with periodic batching.
/// </summary>
/// <remarks>
/// <para>
/// This factory creates a <see cref="SQLiteSink"/> and wraps it with a <see cref="PeriodicBatchingSink"/>
/// to provide automatic batching of log events. The batching behavior is configured from
/// <see cref="SQLiteSinkOptions.BatchSizeLimit"/>, <see cref="SQLiteSinkOptions.BatchPeriod"/>,
/// and <see cref="SQLiteSinkOptions.QueueLimit"/>.
/// </para>
/// <para>
/// The <see cref="PeriodicBatchingSink"/> takes ownership of the underlying <see cref="SQLiteSink"/>
/// and disposes it when the periodic batching sink is disposed.
/// </para>
/// </remarks>
internal static class SQLiteSinkFactory
{
    /// <summary>
    /// Creates a new SQLite sink with periodic batching wrapper.
    /// </summary>
    /// <param name="options">The configuration options for the sink.</param>
    /// <returns>
    /// An <see cref="Serilog.Core.ILogEventSink"/> that batches events and writes them to SQLite.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="options"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when the options fail validation.
    /// </exception>
    /// <remarks>
    /// <para>
    /// The returned sink is a <see cref="PeriodicBatchingSink"/> that wraps the <see cref="SQLiteSink"/>.
    /// Events are buffered and written in batches according to the configured
    /// <see cref="SQLiteSinkOptions.BatchSizeLimit"/> and <see cref="SQLiteSinkOptions.BatchPeriod"/>.
    /// </para>
    /// <para>
    /// If an exception occurs during sink creation after the <see cref="SQLiteSink"/> is created,
    /// the sink is properly disposed before re-throwing the exception.
    /// </para>
    /// </remarks>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "PeriodicBatchingSink takes ownership of the sink and disposes it")]
    public static Serilog.Core.ILogEventSink Create(SQLiteSinkOptions options)
    {
        var sink = new SQLiteSink(options);

        try
        {
            var batchingOptions = new PeriodicBatchingSinkOptions
            {
                BatchSizeLimit = options.BatchSizeLimit,
                Period = options.BatchPeriod,
                QueueLimit = options.QueueLimit
            };

            return new PeriodicBatchingSink(sink, batchingOptions);
        }
        catch
        {
            sink.Dispose();
            throw;
        }
    }
}
