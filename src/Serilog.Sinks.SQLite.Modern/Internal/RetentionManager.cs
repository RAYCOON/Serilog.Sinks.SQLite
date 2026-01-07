// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using System.Globalization;
using Microsoft.Data.Sqlite;
using Serilog.Debugging;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Internal;

/// <summary>
/// Verwaltet die automatische Bereinigung alter Log-Einträge.
/// </summary>
internal sealed class RetentionManager : IDisposable, IAsyncDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly DatabaseManager _databaseManager;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly Task _cleanupTask;
    private bool _disposed;

    public RetentionManager(SQLiteSinkOptions options, DatabaseManager databaseManager)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _databaseManager = databaseManager ?? throw new ArgumentNullException(nameof(databaseManager));
        _cancellationTokenSource = new CancellationTokenSource();

        // Starte Cleanup-Loop wenn mindestens eine Retention-Policy konfiguriert ist
        if (HasRetentionPolicy())
        {
            _cleanupTask = RunCleanupLoopAsync(_cancellationTokenSource.Token);
        }
        else
        {
            _cleanupTask = Task.CompletedTask;
        }
    }

    private bool HasRetentionPolicy()
    {
        return _options.RetentionPeriod.HasValue ||
               _options.RetentionCount.HasValue ||
               _options.MaxDatabaseSize.HasValue;
    }

    /// <summary>
    /// Der Haupt-Cleanup-Loop.
    /// </summary>
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
    /// Führt alle konfigurierten Cleanup-Operationen aus.
    /// </summary>
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
    /// Löscht die ältesten Einträge, wenn die maximale Anzahl überschritten wird.
    /// </summary>
    private async Task<long> CleanupByCountAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _databaseManager.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        
        // Zuerst aktuelle Anzahl prüfen
        await using var countCmd = connection.CreateCommand();
        countCmd.CommandText = $"SELECT COUNT(*) FROM [{_options.TableName}]";
        var currentCount = (long)(await countCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;

        if (currentCount <= _options.RetentionCount!.Value)
        {
            return 0;
        }

        var deleteCount = currentCount - _options.RetentionCount.Value;

        // Lösche die ältesten Einträge
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
    /// Löscht Einträge, wenn die maximale Datenbankgröße überschritten wird.
    /// </summary>
    private async Task<long> CleanupBySizeAsync(CancellationToken cancellationToken)
    {
        var currentSize = await _databaseManager.GetDatabaseSizeAsync(cancellationToken).ConfigureAwait(false);
        
        if (currentSize <= _options.MaxDatabaseSize!.Value)
        {
            return 0;
        }

        // Schätze, wie viele Einträge gelöscht werden müssen
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

        // Optional: VACUUM ausführen um Speicherplatz freizugeben
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
    /// Führt einen manuellen Cleanup aus.
    /// </summary>
    public async Task CleanupNowAsync(CancellationToken cancellationToken = default)
    {
        await PerformCleanupAsync(cancellationToken).ConfigureAwait(false);
    }

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
