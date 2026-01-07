// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using Serilog.Configuration;
using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;
using Serilog.Sinks.SQLite.Modern.Sinks;

namespace Serilog;

/// <summary>
/// Erweiterungsmethoden für die Konfiguration der SQLite-Sink.
/// </summary>
public static class SQLiteLoggerConfigurationExtensions
{
    /// <summary>
    /// Schreibt Log-Events in eine SQLite-Datenbank.
    /// </summary>
    /// <param name="loggerConfiguration">Die Logger-Konfiguration.</param>
    /// <param name="databasePath">Der Pfad zur SQLite-Datenbankdatei.</param>
    /// <param name="tableName">Der Name der Log-Tabelle. Standard: "Logs"</param>
    /// <param name="restrictedToMinimumLevel">Das minimale Log-Level. Standard: Verbose</param>
    /// <param name="storeTimestampInUtc">Zeitstempel in UTC speichern. Standard: true</param>
    /// <param name="batchSizeLimit">Maximale Batch-Größe. Standard: 100</param>
    /// <param name="batchPeriod">Batch-Intervall. Standard: 2 Sekunden</param>
    /// <param name="retentionPeriod">Aufbewahrungsdauer für Log-Einträge. Null = unbegrenzt</param>
    /// <param name="retentionCount">Maximale Anzahl von Log-Einträgen. Null = unbegrenzt</param>
    /// <returns>Die Logger-Konfiguration für Fluent-Chaining.</returns>
    /// <example>
    /// <code>
    /// var logger = new LoggerConfiguration()
    ///     .WriteTo.SQLite(
    ///         databasePath: "logs/app.db",
    ///         tableName: "AppLogs",
    ///         retentionPeriod: TimeSpan.FromDays(30))
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
    /// Schreibt Log-Events in eine SQLite-Datenbank mit erweiterten Konfigurationsoptionen.
    /// </summary>
    /// <param name="loggerConfiguration">Die Logger-Konfiguration.</param>
    /// <param name="databasePath">Der Pfad zur SQLite-Datenbankdatei.</param>
    /// <param name="configure">Aktion zur Konfiguration der erweiterten Optionen.</param>
    /// <returns>Die Logger-Konfiguration für Fluent-Chaining.</returns>
    /// <example>
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
    /// Schreibt Log-Events in eine SQLite-Datenbank mit vollständigem Options-Objekt.
    /// </summary>
    /// <param name="loggerConfiguration">Die Logger-Konfiguration.</param>
    /// <param name="options">Die vollständigen Konfigurationsoptionen.</param>
    /// <returns>Die Logger-Konfiguration für Fluent-Chaining.</returns>
    /// <example>
    /// <code>
    /// var options = new SQLiteSinkOptions
    /// {
    ///     DatabasePath = "logs/app.db",
    ///     TableName = "Logs",
    ///     RetentionPeriod = TimeSpan.FromDays(7),
    ///     BatchSizeLimit = 200
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
