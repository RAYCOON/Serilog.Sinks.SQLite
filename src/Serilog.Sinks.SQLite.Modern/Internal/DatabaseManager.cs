// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Serilog.Debugging;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Internal;

/// <summary>
/// Verwaltet die SQLite-Datenbankverbindungen und das Schema.
/// </summary>
internal sealed class DatabaseManager : IDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly string _connectionString;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _schemaInitialized;
    private bool _disposed;

    /// <summary>
    /// Die Standard-Spaltennamen der Log-Tabelle.
    /// </summary>
    public static class Columns
    {
        public const string Id = "Id";
        public const string Timestamp = "Timestamp";
        public const string Level = "Level";
        public const string LevelName = "LevelName";
        public const string Message = "Message";
        public const string MessageTemplate = "MessageTemplate";
        public const string Exception = "Exception";
        public const string Properties = "Properties";
        public const string SourceContext = "SourceContext";
        public const string MachineName = "MachineName";
        public const string ThreadId = "ThreadId";
    }

    public DatabaseManager(SQLiteSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _connectionString = BuildConnectionString(options);
    }

    /// <summary>
    /// Baut den Connection-String basierend auf den Optionen.
    /// </summary>
    private static string BuildConnectionString(SQLiteSinkOptions options)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = options.DatabasePath,
            Mode = options.DatabasePath == ":memory:"
                ? SqliteOpenMode.Memory
                : SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        };

        // Zusätzliche Parameter hinzufügen
        foreach (var param in options.AdditionalConnectionParameters)
        {
            builder[param.Key] = param.Value;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Erstellt eine neue Datenbankverbindung.
    /// </summary>
    public SqliteConnection CreateConnection()
    {
        return new SqliteConnection(_connectionString);
    }

    /// <summary>
    /// Öffnet eine Verbindung und konfiguriert sie.
    /// </summary>
    public async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connection = CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ConfigureConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Konfiguriert eine geöffnete Verbindung mit den Pragma-Einstellungen.
    /// </summary>
    private async Task ConfigureConnectionAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        // Journal-Mode setzen
        var journalMode = _options.JournalMode.ToString().ToUpperInvariant();
        await ExecutePragmaAsync(connection, string.Create(CultureInfo.InvariantCulture, $"journal_mode = {journalMode}"), cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, string.Create(CultureInfo.InvariantCulture, $"synchronous = {(int)_options.SynchronousMode}"), cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, "temp_store = MEMORY", cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, "mmap_size = 268435456", cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, "cache_size = -64000", cancellationToken).ConfigureAwait(false);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security",
        "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "PRAGMA values come from internal options, not user input")]
    private static async Task ExecutePragmaAsync(SqliteConnection connection, string pragma, CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = string.Concat("PRAGMA ", pragma);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Initialisiert das Datenbankschema, wenn noch nicht geschehen.
    /// </summary>
    public async Task EnsureSchemaAsync(CancellationToken cancellationToken = default)
    {
        if (_schemaInitialized || !_options.AutoCreateDatabase)
        {
            return;
        }

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_schemaInitialized)
            {
                return;
            }

            // Verzeichnis erstellen falls nötig
            EnsureDirectoryExists();

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await CreateTableAsync(connection, cancellationToken).ConfigureAwait(false);
            await CreateIndexesAsync(connection, cancellationToken).ConfigureAwait(false);

            _schemaInitialized = true;
            SelfLog.WriteLine("SQLite schema initialized successfully for table '{0}'", _options.TableName);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private void EnsureDirectoryExists()
    {
        if (_options.DatabasePath == ":memory:")
        {
            return;
        }

        var directory = Path.GetDirectoryName(_options.DatabasePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
            SelfLog.WriteLine("Created directory '{0}' for SQLite database", directory);
        }
    }

    /// <summary>
    /// Erstellt die Log-Tabelle.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security",
        "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "SQL is built from validated internal options, not user input")]
    private async Task CreateTableAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        var sql = BuildCreateTableSql();

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Baut das CREATE TABLE Statement.
    /// </summary>
    private string BuildCreateTableSql()
    {
        var sb = new StringBuilder();
        sb.Append(CultureInfo.InvariantCulture, $"CREATE TABLE IF NOT EXISTS [{_options.TableName}] (");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.Id}] INTEGER PRIMARY KEY AUTOINCREMENT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.Timestamp}] TEXT NOT NULL,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.Level}] INTEGER NOT NULL,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.LevelName}] TEXT NOT NULL,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.Message}] TEXT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.MessageTemplate}] TEXT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.Exception}] TEXT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.Properties}] TEXT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.SourceContext}] TEXT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.MachineName}] TEXT,");
        sb.AppendLine();
        sb.Append(CultureInfo.InvariantCulture, $"    [{Columns.ThreadId}] INTEGER");

        foreach (var column in _options.CustomColumns)
        {
            var nullable = column.AllowNull ? "" : " NOT NULL";
            sb.AppendLine();
            sb.Append(CultureInfo.InvariantCulture, $"    ,[{column.ColumnName}] {column.DataType}{nullable}");
        }

        sb.AppendLine();
        sb.Append(");");
        return sb.ToString();
    }

    /// <summary>
    /// Erstellt Indizes für bessere Query-Performance.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security",
        "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "SQL is built from validated internal options, not user input")]
    private async Task CreateIndexesAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        var indexes = new List<(string Name, string[] Columns)>
        {
            ($"IX_{_options.TableName}_Timestamp", [Columns.Timestamp]),
            ($"IX_{_options.TableName}_Level", [Columns.Level]),
            ($"IX_{_options.TableName}_Timestamp_Level", [Columns.Timestamp, Columns.Level])
        };

        // Custom Column Indexes
        foreach (var column in _options.CustomColumns.Where(c => c.CreateIndex))
        {
            indexes.Add(($"IX_{_options.TableName}_{column.ColumnName}", [column.ColumnName]));
        }

        foreach (var (name, columns) in indexes)
        {
            await using var cmd = connection.CreateCommand();
            var columnList = string.Join(", ", columns.Select(c => $"[{c}]"));
            cmd.CommandText = $"CREATE INDEX IF NOT EXISTS [{name}] ON [{_options.TableName}] ({columnList})";
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Gibt die aktuelle Datenbankgröße in Bytes zurück.
    /// </summary>
    public async Task<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default)
    {
        if (_options.DatabasePath == ":memory:")
        {
            return 0;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // Page count abfragen
        await using var pageCountCmd = connection.CreateCommand();
        pageCountCmd.CommandText = "PRAGMA page_count";
        var pageCountResult = await pageCountCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        var pageCount = Convert.ToInt64(pageCountResult, CultureInfo.InvariantCulture);

        await using var pageSizeCmd = connection.CreateCommand();
        pageSizeCmd.CommandText = "PRAGMA page_size";
        var pageSizeResult = await pageSizeCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        var pageSize = Convert.ToInt64(pageSizeResult, CultureInfo.InvariantCulture);

        return pageCount * pageSize;
    }

    /// <summary>
    /// Gibt die Anzahl der Log-Einträge zurück.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security",
        "CA2100:Review SQL queries for security vulnerabilities",
        Justification = "TableName comes from validated internal options, not user input")]
    public async Task<long> GetLogCountAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = string.Create(CultureInfo.InvariantCulture, $"SELECT COUNT(*) FROM [{_options.TableName}]");

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long count ? count : 0;
    }

    /// <summary>
    /// Führt VACUUM aus, um die Datenbankdatei zu komprimieren.
    /// </summary>
    public async Task VacuumAsync(CancellationToken cancellationToken = default)
    {
        if (_options.DatabasePath == ":memory:")
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "VACUUM";
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        SelfLog.WriteLine("SQLite VACUUM completed for database '{0}'", _options.DatabasePath);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _semaphore.Dispose();
        _disposed = true;
    }
}
