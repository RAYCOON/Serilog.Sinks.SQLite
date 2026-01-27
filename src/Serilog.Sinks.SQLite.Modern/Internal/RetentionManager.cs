// Copyright (c) 2025 RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Globalization;
using Microsoft.Data.Sqlite;
using Serilog.Debugging;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Internal;

/// <summary>
/// Manages automatic cleanup of old log entries based on configured retention policies.
/// </summary>
/// <remarks>
/// <para>
/// This class implements background cleanup of log entries based on three types of retention policies:
/// </para>
/// <list type="bullet">
///   <item>
///     <description>
///       <b>Time-based retention</b>: Deletes entries older than <see cref="SQLiteSinkOptions.RetentionPeriod"/>.
///     </description>
///   </item>
///   <item>
///     <description>
///       <b>Count-based retention</b>: Keeps only the most recent <see cref="SQLiteSinkOptions.RetentionCount"/> entries.
///     </description>
///   </item>
///   <item>
///     <description>
///       <b>Size-based retention</b>: Deletes oldest entries when database exceeds <see cref="SQLiteSinkOptions.MaxDatabaseSize"/>.
///     </description>
///   </item>
/// </list>
/// <para>
/// The cleanup loop runs in the background at intervals defined by <see cref="SQLiteSinkOptions.CleanupInterval"/>.
/// It starts with an initial delay of 1 minute to allow the application to start up before performing
/// potentially expensive cleanup operations.
/// </para>
/// <para>
/// When size-based retention deletes more than 1000 entries, a VACUUM operation is automatically
/// triggered to reclaim disk space.
/// </para>
/// </remarks>
internal sealed class RetentionManager : IDisposable, IAsyncDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly DatabaseManager _databaseManager;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task _cleanupTask;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RetentionManager"/> class and starts the cleanup loop.
    /// </summary>
    /// <param name="options">
    /// The sink configuration options containing retention policies and cleanup interval settings.
    /// Must not be <c>null</c>.
    /// </param>
    /// <param name="databaseManager">
    /// The database manager for executing cleanup queries. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="options"/> or <paramref name="databaseManager"/> is <c>null</c>.
    /// </exception>
    /// <remarks>
    /// The cleanup loop is only started if at least one retention policy is configured
    /// (<see cref="SQLiteSinkOptions.RetentionPeriod"/>, <see cref="SQLiteSinkOptions.RetentionCount"/>,
    /// or <see cref="SQLiteSinkOptions.MaxDatabaseSize"/>).
    /// </remarks>
    public RetentionManager(SQLiteSinkOptions options, DatabaseManager databaseManager)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _databaseManager = databaseManager ?? throw new ArgumentNullException(nameof(databaseManager));
        _cancellationTokenSource = new CancellationTokenSource();

        // Start cleanup loop if at least one retention policy is configured
        if (HasRetentionPolicy())
        {
            _cleanupTask = RunCleanupLoopAsync(_cancellationTokenSource.Token);
        }
        else
        {
            _cleanupTask = Task.CompletedTask;
        }
    }

    /// <summary>
    /// Determines whether any retention policy is configured.
    /// </summary>
    /// <returns><c>true</c> if at least one retention policy is set; otherwise, <c>false</c>.</returns>
    private bool HasRetentionPolicy()
    {
        return _options.RetentionPeriod.HasValue ||
               _options.RetentionCount.HasValue ||
               _options.MaxDatabaseSize.HasValue;
    }

    /// <summary>
    /// The main background cleanup loop that periodically executes cleanup operations.
    /// </summary>
    /// <param name="cancellationToken">A token to signal when the loop should stop.</param>
    /// <returns>A task representing the background cleanup operation.</returns>
    /// <remarks>
    /// The loop starts with a 1-minute initial delay, then runs cleanup at the configured
    /// <see cref="SQLiteSinkOptions.CleanupInterval"/>. Errors during cleanup are logged
    /// but do not stop the loop.
    /// </remarks>
    private async Task RunCleanupLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        using var timer = new PeriodicTimer(_options.CleanupInterval);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await PerformCleanupAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("SQLite retention cleanup error: {0}", ex);
            }

            try
            {
                await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>
    /// Executes all configured cleanup operations in sequence.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the cleanup operation.</param>
    /// <returns>A task representing the cleanup operation.</returns>
    /// <remarks>
    /// Cleanup operations are executed in the following order:
    /// <list type="number">
    ///   <item><description>Time-based cleanup (RetentionPeriod)</description></item>
    ///   <item><description>Count-based cleanup (RetentionCount)</description></item>
    ///   <item><description>Size-based cleanup (MaxDatabaseSize)</description></item>
    /// </list>
    /// If any cleanup operation fails, the error is logged and the <see cref="SQLiteSinkOptions.OnError"/>
    /// callback is invoked if configured.
    /// </remarks>
    private async Task PerformCleanupAsync(CancellationToken cancellationToken)
    {
        var deletedCount = 0L;

        try
        {
            // Retention by Period
            if (_options.RetentionPeriod.HasValue)
            {
                deletedCount += await CleanupByPeriodAsync(cancellationToken).ConfigureAwait(false);
            }

            // Retention by Count
            if (_options.RetentionCount.HasValue)
            {
                deletedCount += await CleanupByCountAsync(cancellationToken).ConfigureAwait(false);
            }

            // Retention by Database Size
            if (_options.MaxDatabaseSize.HasValue)
            {
                deletedCount += await CleanupBySizeAsync(cancellationToken).ConfigureAwait(false);
            }

            if (deletedCount > 0)
            {
                SelfLog.WriteLine("SQLite retention cleanup completed: {0} entries deleted", deletedCount);
            }
        }
        catch (Exception ex)
        {
            SelfLog.WriteLine("SQLite retention cleanup failed: {0}", ex);
            _options.OnError?.Invoke(ex);
        }
    }

    /// <summary>
    /// Deletes log entries older than the configured retention period.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the number of deleted entries.</returns>
    /// <remarks>
    /// The cutoff date is calculated based on <see cref="SQLiteSinkOptions.StoreTimestampInUtc"/>
    /// to match the timestamp format used when storing entries.
    /// </remarks>
    private async Task<long> CleanupByPeriodAsync(CancellationToken cancellationToken)
    {
        var cutoffDate = (_options.StoreTimestampInUtc ? DateTime.UtcNow : DateTime.Now)
            - _options.RetentionPeriod!.Value;

        await using var connection = await _databaseManager.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            DELETE FROM [{_options.TableName}]
            WHERE [{DatabaseManager.Columns.Timestamp}] < @cutoffDate";
        cmd.Parameters.AddWithValue("@cutoffDate", cutoffDate.ToString("O", CultureInfo.InvariantCulture));

        return await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes the oldest log entries to maintain the configured maximum count.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the number of deleted entries.</returns>
    /// <remarks>
    /// Only deletes entries if the current count exceeds <see cref="SQLiteSinkOptions.RetentionCount"/>.
    /// Entries are deleted in order of timestamp (oldest first).
    /// </remarks>
    private async Task<long> CleanupByCountAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _databaseManager.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // First check current count
        await using var countCmd = connection.CreateCommand();
        countCmd.CommandText = $"SELECT COUNT(*) FROM [{_options.TableName}]";
        var currentCount = (long)(await countCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;

        if (currentCount <= _options.RetentionCount!.Value)
        {
            return 0;
        }

        var deleteCount = currentCount - _options.RetentionCount.Value;

        // Delete the oldest entries
        await using var deleteCmd = connection.CreateCommand();
        deleteCmd.CommandText = $@"
            DELETE FROM [{_options.TableName}]
            WHERE [{DatabaseManager.Columns.Id}] IN (
                SELECT [{DatabaseManager.Columns.Id}]
                FROM [{_options.TableName}]
                ORDER BY [{DatabaseManager.Columns.Timestamp}] ASC
                LIMIT @limit
            )";
        deleteCmd.Parameters.AddWithValue("@limit", deleteCount);

        return await deleteCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes the oldest log entries to maintain the database under the configured maximum size.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the number of deleted entries.</returns>
    /// <remarks>
    /// <para>
    /// The cleanup targets 80% of the maximum database size to provide headroom and avoid
    /// frequent cleanups. The number of entries to delete is estimated based on the average
    /// size per entry.
    /// </para>
    /// <para>
    /// If more than 1000 entries are deleted, a VACUUM operation is automatically performed
    /// to reclaim the disk space from the deleted entries.
    /// </para>
    /// </remarks>
    private async Task<long> CleanupBySizeAsync(CancellationToken cancellationToken)
    {
        var currentSize = await _databaseManager.GetDatabaseSizeAsync(cancellationToken).ConfigureAwait(false);

        if (currentSize <= _options.MaxDatabaseSize!.Value)
        {
            return 0;
        }

        // Estimate how many entries need to be deleted
        var currentCount = await _databaseManager.GetLogCountAsync(cancellationToken).ConfigureAwait(false);
        if (currentCount == 0)
        {
            return 0;
        }

        var avgSizePerEntry = (double)currentSize / currentCount;
        var targetSize = _options.MaxDatabaseSize.Value * 0.8; // 80% of max
        var targetCount = (long)(targetSize / avgSizePerEntry);
        var deleteCount = currentCount - targetCount;

        if (deleteCount <= 0)
        {
            return 0;
        }

        await using var connection = await _databaseManager.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
            DELETE FROM [{_options.TableName}]
            WHERE [{DatabaseManager.Columns.Id}] IN (
                SELECT [{DatabaseManager.Columns.Id}]
                FROM [{_options.TableName}]
                ORDER BY [{DatabaseManager.Columns.Timestamp}] ASC
                LIMIT @limit
            )";
        cmd.Parameters.AddWithValue("@limit", deleteCount);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        // Optional: Execute VACUUM to reclaim disk space
        if (deleted > 1000)
        {
            try
            {
                await _databaseManager.VacuumAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("SQLite VACUUM failed: {0}", ex);
            }
        }

        return deleted;
    }

    /// <summary>
    /// Triggers an immediate cleanup operation outside of the regular schedule.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the cleanup operation.</returns>
    /// <remarks>
    /// This method can be called to force a cleanup at any time, regardless of the
    /// configured <see cref="SQLiteSinkOptions.CleanupInterval"/>. Useful for testing
    /// or when you need to immediately reclaim space.
    /// </remarks>
    public async Task CleanupNowAsync(CancellationToken cancellationToken = default)
    {
        await PerformCleanupAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases all resources used by the <see cref="RetentionManager"/> and stops the cleanup loop.
    /// </summary>
    /// <remarks>
    /// This method cancels the background cleanup task and waits for it to complete.
    /// It blocks until the cleanup task has finished.
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _cancellationTokenSource.Cancel();

        try
        {
            _cleanupTask.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        _cancellationTokenSource.Dispose();
        _disposed = true;
    }

    /// <summary>
    /// Asynchronously releases all resources used by the <see cref="RetentionManager"/> and stops the cleanup loop.
    /// </summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    /// <remarks>
    /// This method cancels the background cleanup task and asynchronously waits for it to complete.
    /// Prefer this method over <see cref="Dispose"/> when disposing in an async context.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);

        try
        {
            await _cleanupTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        _cancellationTokenSource.Dispose();
        _disposed = true;
    }
}
