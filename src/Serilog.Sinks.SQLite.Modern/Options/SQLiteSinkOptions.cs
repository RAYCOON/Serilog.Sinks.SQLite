// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using System.Collections.ObjectModel;
using Serilog.Events;

namespace Serilog.Sinks.SQLite.Modern.Options;

/// <summary>
/// Konfigurationsoptionen für die SQLite Serilog Sink.
/// </summary>
public sealed class SQLiteSinkOptions
{
    private readonly Collection<CustomColumn> _customColumns = [];
    private readonly Dictionary<string, string> _additionalConnectionParameters = [];

    /// <summary>
    /// Der Pfad zur SQLite-Datenbankdatei.
    /// Kann ein relativer oder absoluter Pfad sein.
    /// </summary>
    /// <example>
    /// <code>
    /// // Relativer Pfad
    /// options.DatabasePath = "logs/app.db";
    /// 
    /// // Absoluter Pfad
    /// options.DatabasePath = "/var/log/myapp/app.db";
    /// 
    /// // In-Memory (nicht empfohlen für Produktion)
    /// options.DatabasePath = ":memory:";
    /// </code>
    /// </example>
    public string DatabasePath { get; set; } = "logs.db";

    /// <summary>
    /// Der Name der Log-Tabelle in der Datenbank.
    /// </summary>
    public string TableName { get; set; } = "Logs";

    /// <summary>
    /// Gibt an, ob Zeitstempel in UTC gespeichert werden sollen.
    /// Standard ist <c>true</c>.
    /// </summary>
    public bool StoreTimestampInUtc { get; set; } = true;

    /// <summary>
    /// Das minimale Log-Level, das geschrieben werden soll.
    /// </summary>
    public LogEventLevel RestrictedToMinimumLevel { get; set; } = LogEventLevel.Verbose;

    /// <summary>
    /// Die maximale Anzahl von Log-Einträgen, die in der Datenbank gespeichert werden.
    /// Bei Überschreitung werden die ältesten Einträge gelöscht.
    /// <c>null</c> bedeutet unbegrenzt.
    /// </summary>
    public long? RetentionCount { get; set; }

    /// <summary>
    /// Die maximale Aufbewahrungsdauer für Log-Einträge.
    /// Ältere Einträge werden automatisch gelöscht.
    /// <c>null</c> bedeutet unbegrenzte Aufbewahrung.
    /// </summary>
    public TimeSpan? RetentionPeriod { get; set; }

    /// <summary>
    /// Die maximale Größe der Datenbankdatei in Bytes.
    /// Bei Überschreitung werden die ältesten Einträge gelöscht.
    /// <c>null</c> bedeutet unbegrenzt.
    /// </summary>
    public long? MaxDatabaseSize { get; set; }

    /// <summary>
    /// Intervall für die Bereinigung alter Log-Einträge.
    /// Standard ist 1 Stunde.
    /// </summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Die Anzahl der Log-Events, die in einem Batch geschrieben werden.
    /// </summary>
    public int BatchSizeLimit { get; set; } = 100;

    /// <summary>
    /// Das Intervall, in dem Batches geschrieben werden.
    /// </summary>
    public TimeSpan BatchPeriod { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Die maximale Anzahl von Events in der Warteschlange.
    /// Wenn die Warteschlange voll ist, werden neue Events verworfen.
    /// <c>null</c> bedeutet unbegrenzt.
    /// </summary>
    public int? QueueLimit { get; set; } = 10000;

    /// <summary>
    /// Gibt an, ob die Datenbank und Tabelle automatisch erstellt werden sollen.
    /// Standard ist <c>true</c>.
    /// </summary>
    public bool AutoCreateDatabase { get; set; } = true;

    /// <summary>
    /// Gibt an, ob Properties als JSON gespeichert werden sollen.
    /// Standard ist <c>true</c>.
    /// </summary>
    public bool StorePropertiesAsJson { get; set; } = true;

    /// <summary>
    /// Die SQLite-Journal-Mode Einstellung.
    /// Standard ist WAL (Write-Ahead Logging) für bessere Performance.
    /// </summary>
    public SQLiteJournalMode JournalMode { get; set; } = SQLiteJournalMode.Wal;

    /// <summary>
    /// Die SQLite-Synchronous Einstellung.
    /// Standard ist Normal für gute Balance zwischen Performance und Sicherheit.
    /// </summary>
    public SQLiteSynchronousMode SynchronousMode { get; set; } = SQLiteSynchronousMode.Normal;

    /// <summary>
    /// Zusätzliche Connection-String-Parameter (read-only collection).
    /// </summary>
    public IDictionary<string, string> AdditionalConnectionParameters => _additionalConnectionParameters;

    /// <summary>
    /// Callback, der aufgerufen wird, wenn ein Fehler beim Schreiben auftritt.
    /// </summary>
    public Action<Exception>? OnError { get; set; }

    /// <summary>
    /// Gibt an, ob das Exception-Detail gespeichert werden soll.
    /// Standard ist <c>true</c>.
    /// </summary>
    public bool StoreExceptionDetails { get; set; } = true;

    /// <summary>
    /// Die maximale Länge der Exception-Details in Zeichen.
    /// <c>null</c> bedeutet keine Begrenzung.
    /// </summary>
    public int? MaxExceptionLength { get; set; }

    /// <summary>
    /// Die maximale Länge der Nachricht in Zeichen.
    /// <c>null</c> bedeutet keine Begrenzung.
    /// </summary>
    public int? MaxMessageLength { get; set; }

    /// <summary>
    /// Die maximale Länge der Properties in Zeichen.
    /// <c>null</c> bedeutet keine Begrenzung.
    /// </summary>
    public int? MaxPropertiesLength { get; set; }

    /// <summary>
    /// Gibt an, ob bei einem Fehler eine Exception geworfen werden soll.
    /// </summary>
    public bool ThrowOnError { get; set; }

    /// <summary>
    /// Zusätzliche Spalten, die in der Log-Tabelle erstellt werden sollen.
    /// </summary>
    public Collection<CustomColumn> CustomColumns => _customColumns;

    /// <summary>
    /// Validiert die Optionen und wirft eine Exception bei ungültigen Werten.
    /// </summary>
    /// <exception cref="ArgumentException">Bei ungültigen Optionen.</exception>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(DatabasePath))
        {
            throw new ArgumentException("DatabasePath darf nicht leer sein.", nameof(DatabasePath));
        }

        if (string.IsNullOrWhiteSpace(TableName))
        {
            throw new ArgumentException("TableName darf nicht leer sein.", nameof(TableName));
        }

        if (BatchSizeLimit <= 0)
        {
            throw new ArgumentException("BatchSizeLimit muss größer als 0 sein.", nameof(BatchSizeLimit));
        }

        if (BatchPeriod <= TimeSpan.Zero)
        {
            throw new ArgumentException("BatchPeriod muss größer als 0 sein.", nameof(BatchPeriod));
        }

        if (QueueLimit.HasValue && QueueLimit.Value <= 0)
        {
            throw new ArgumentException("QueueLimit muss größer als 0 sein.", nameof(QueueLimit));
        }

        if (RetentionCount.HasValue && RetentionCount.Value <= 0)
        {
            throw new ArgumentException("RetentionCount muss größer als 0 sein.", nameof(RetentionCount));
        }

        if (RetentionPeriod.HasValue && RetentionPeriod.Value <= TimeSpan.Zero)
        {
            throw new ArgumentException("RetentionPeriod muss größer als 0 sein.", nameof(RetentionPeriod));
        }

        if (MaxDatabaseSize.HasValue && MaxDatabaseSize.Value <= 0)
        {
            throw new ArgumentException("MaxDatabaseSize muss größer als 0 sein.", nameof(MaxDatabaseSize));
        }
    }

    /// <summary>
    /// Erstellt eine Kopie der Optionen.
    /// </summary>
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
/// SQLite Journal-Mode Einstellungen.
/// </summary>
public enum SQLiteJournalMode
{
    /// <summary>
    /// Rollback-Journal wird gelöscht nach Commit.
    /// </summary>
    Delete,

    /// <summary>
    /// Rollback-Journal wird auf Null-Bytes gesetzt.
    /// </summary>
    Truncate,

    /// <summary>
    /// Rollback-Journal bleibt bestehen (Header wird invalidiert).
    /// </summary>
    Persist,

    /// <summary>
    /// Journal wird im Speicher gehalten.
    /// </summary>
    Memory,

    /// <summary>
    /// Write-Ahead Logging - empfohlen für beste Performance.
    /// </summary>
    Wal,

    /// <summary>
    /// Kein Journal - nur für read-only oder temporäre Datenbanken.
    /// </summary>
    Off
}

/// <summary>
/// SQLite Synchronous-Mode Einstellungen.
/// </summary>
public enum SQLiteSynchronousMode
{
    /// <summary>
    /// Keine Synchronisation - schnellster Modus, aber unsicher bei Systemabsturz.
    /// </summary>
    Off = 0,

    /// <summary>
    /// Normale Synchronisation - gute Balance.
    /// </summary>
    Normal = 1,

    /// <summary>
    /// Vollständige Synchronisation - sicherster Modus, aber langsamer.
    /// </summary>
    Full = 2,

    /// <summary>
    /// Extra sichere Synchronisation.
    /// </summary>
    Extra = 3
}

/// <summary>
/// Definition einer benutzerdefinierten Spalte.
/// </summary>
public sealed class CustomColumn
{
    /// <summary>
    /// Der Name der Spalte.
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// Der SQLite-Datentyp der Spalte.
    /// </summary>
    public required string DataType { get; init; }

    /// <summary>
    /// Der Name der Serilog-Property.
    /// </summary>
    public required string PropertyName { get; init; }

    /// <summary>
    /// Gibt an, ob die Spalte NULL-Werte erlaubt.
    /// </summary>
    public bool AllowNull { get; init; } = true;

    /// <summary>
    /// Gibt an, ob ein Index erstellt werden soll.
    /// </summary>
    public bool CreateIndex { get; init; }
}
