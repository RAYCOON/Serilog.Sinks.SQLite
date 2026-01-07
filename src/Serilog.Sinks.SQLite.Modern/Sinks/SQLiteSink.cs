// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.PeriodicBatching;
using Serilog.Sinks.SQLite.Modern.Internal;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Sinks;

/// <summary>
/// Eine Serilog-Sink, die Log-Events in eine SQLite-Datenbank schreibt.
/// </summary>
/// <remarks>
/// Diese Sink verwendet Batching für optimale Performance und unterstützt
/// automatische Retention-Policies, Custom Columns und verschiedene
/// Konfigurationsoptionen.
/// </remarks>
/// <example>
/// <code>
/// var logger = new LoggerConfiguration()
///     .WriteTo.SQLite("logs/app.db", options =>
///     {
///         options.TableName = "ApplicationLogs";
///         options.RetentionPeriod = TimeSpan.FromDays(30);
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
    /// Erstellt eine neue Instanz der SQLite-Sink.
    /// </summary>
    /// <param name="options">Die Konfigurationsoptionen.</param>
    /// <exception cref="ArgumentNullException">Wenn options null ist.</exception>
    /// <exception cref="ArgumentException">Wenn die Optionen ungültig sind.</exception>
    public SQLiteSink(SQLiteSinkOptions options)
    {
        _options = options?.Clone() ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();

        _databaseManager = new DatabaseManager(_options);
        _batchWriter = new LogEventBatchWriter(_options, _databaseManager);

        // Retention Manager nur erstellen, wenn Retention konfiguriert ist
        // Der Manager startet automatisch im Konstruktor
        if (_options.RetentionPeriod.HasValue || 
            _options.RetentionCount.HasValue || 
            _options.MaxDatabaseSize.HasValue)
        {
            _retentionManager = new RetentionManager(_options, _databaseManager);
        }

        SelfLog.WriteLine("SQLite sink initialized: {0}", _options.DatabasePath);
    }

    /// <summary>
    /// Schreibt einen Batch von Log-Events asynchron.
    /// </summary>
    /// <param name="batch">Die zu schreibenden Events.</param>
    /// <returns>Ein Task, der die Operation repräsentiert.</returns>
    public async Task EmitBatchAsync(IEnumerable<LogEvent> batch)
    {
        if (_disposed)
        {
            return;
        }

        await _batchWriter.WriteBatchAsync(batch).ConfigureAwait(false);
    }

    /// <summary>
    /// Wird aufgerufen, wenn die Warteschlange leer ist.
    /// </summary>
    /// <returns>Ein Task, der die Operation repräsentiert.</returns>
    public Task OnEmptyBatchAsync()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gibt die aktuelle Anzahl der Log-Einträge in der Datenbank zurück.
    /// </summary>
    public async Task<long> GetLogCountAsync(CancellationToken cancellationToken = default)
    {
        return await _databaseManager.GetLogCountAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gibt die aktuelle Datenbankgröße in Bytes zurück.
    /// </summary>
    public async Task<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default)
    {
        return await _databaseManager.GetDatabaseSizeAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Führt einen manuellen Cleanup der Retention-Policies aus.
    /// </summary>
    public async Task CleanupAsync(CancellationToken cancellationToken = default)
    {
        if (_retentionManager != null)
        {
            await _retentionManager.CleanupNowAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Führt VACUUM aus, um die Datenbankdatei zu optimieren.
    /// </summary>
    public async Task VacuumAsync(CancellationToken cancellationToken = default)
    {
        await _databaseManager.VacuumAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gibt alle Ressourcen frei.
    /// </summary>
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
    /// Gibt alle Ressourcen asynchron frei.
    /// </summary>
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
/// Factory für die Erstellung der SQLite-Sink mit PeriodicBatching.
/// </summary>
internal static class SQLiteSinkFactory
{
    /// <summary>
    /// Erstellt eine neue SQLite-Sink mit Batching-Wrapper.
    /// </summary>
    /// <remarks>
    /// CA2000 wird unterdrückt, da PeriodicBatchingSink die Ownership der Sink übernimmt
    /// und diese beim Dispose korrekt freigibt.
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
