// Copyright (c) 2025 RAYCOON.com GmbH. All rights reserved.
// Author: Daniel Pavic
// Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;
using Serilog.Debugging;
using Serilog.Sinks.SQLite.Modern.Options;

namespace Serilog.Sinks.SQLite.Modern.Internal;

/// <summary>
/// Manages SQLite database connections, schema initialization, and database operations.
/// </summary>
/// <remarks>
/// <para>
/// This class is responsible for all low-level database operations including:
/// </para>
/// <list type="bullet">
///   <item><description>Building and managing connection strings</description></item>
///   <item><description>Opening and configuring database connections with optimal PRAGMA settings</description></item>
///   <item><description>Creating and initializing the log table schema</description></item>
///   <item><description>Creating indexes for improved query performance</description></item>
///   <item><description>Providing utility methods for database size and log count queries</description></item>
///   <item><description>Executing VACUUM operations for database optimization</description></item>
/// </list>
/// <para>
/// The class uses connection pooling via Microsoft.Data.Sqlite for efficient connection reuse.
/// Schema initialization is performed lazily on first write and is thread-safe using a semaphore.
/// </para>
/// </remarks>
internal sealed class DatabaseManager : IDisposable
{
    private readonly SQLiteSinkOptions _options;
    private readonly string _connectionString;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private bool _schemaInitialized;
    private bool _disposed;

    /// <summary>
    /// Contains constant definitions for the standard column names used in the log table.
    /// </summary>
    /// <remarks>
    /// These column names are used throughout the sink for consistency when building
    /// SQL statements and referencing log data. Custom columns defined in
    /// <see cref="SQLiteSinkOptions.CustomColumns"/> are added in addition to these.
    /// </remarks>
    public static class Columns
    {
        /// <summary>The auto-incrementing primary key column.</summary>
        public const string Id = "Id";

        /// <summary>The timestamp column storing when the log event occurred (ISO 8601 format).</summary>
        public const string Timestamp = "Timestamp";

        /// <summary>The numeric log level (0=Verbose, 1=Debug, 2=Information, 3=Warning, 4=Error, 5=Fatal).</summary>
        public const string Level = "Level";

        /// <summary>The human-readable log level name.</summary>
        public const string LevelName = "LevelName";

        /// <summary>The rendered log message with property values substituted.</summary>
        public const string Message = "Message";

        /// <summary>The original message template before property substitution.</summary>
        public const string MessageTemplate = "MessageTemplate";

        /// <summary>The exception details if an exception was logged (may be null).</summary>
        public const string Exception = "Exception";

        /// <summary>The log event properties serialized as JSON (may be null).</summary>
        public const string Properties = "Properties";

        /// <summary>The source context (typically the class name) that generated the log.</summary>
        public const string SourceContext = "SourceContext";

        /// <summary>The name of the machine where the log was generated.</summary>
        public const string MachineName = "MachineName";

        /// <summary>The managed thread ID that generated the log event.</summary>
        public const string ThreadId = "ThreadId";
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DatabaseManager"/> class.
    /// </summary>
    /// <param name="options">
    /// The sink configuration options containing database path, connection parameters,
    /// and schema settings. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="options"/> is <c>null</c>.
    /// </exception>
    public DatabaseManager(SQLiteSinkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _connectionString = BuildConnectionString(options);
    }

    /// <summary>
    /// Builds the SQLite connection string based on the provided configuration options.
    /// </summary>
    /// <param name="options">The sink options containing database path and connection parameters.</param>
    /// <returns>A fully constructed connection string for SQLite.</returns>
    /// <remarks>
    /// The connection string is configured with:
    /// <list type="bullet">
    ///   <item><description>Shared cache mode for better concurrency</description></item>
    ///   <item><description>Connection pooling enabled for performance</description></item>
    ///   <item><description>Memory mode for <c>:memory:</c> databases</description></item>
    ///   <item><description>ReadWriteCreate mode for file-based databases</description></item>
    ///   <item><description>Any additional parameters from <see cref="SQLiteSinkOptions.AdditionalConnectionParameters"/></description></item>
    /// </list>
    /// </remarks>
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

        // Add any additional connection parameters
        foreach (var param in options.AdditionalConnectionParameters)
        {
            builder[param.Key] = param.Value;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Creates a new, unopened database connection using the configured connection string.
    /// </summary>
    /// <returns>A new <see cref="SqliteConnection"/> instance that has not been opened.</returns>
    /// <remarks>
    /// The caller is responsible for opening and disposing the returned connection.
    /// For most use cases, prefer <see cref="OpenConnectionAsync"/> which handles
    /// opening and configuring the connection.
    /// </remarks>
    public SqliteConnection CreateConnection()
    {
        return new SqliteConnection(_connectionString);
    }

    /// <summary>
    /// Opens a new database connection and configures it with optimal PRAGMA settings.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// an open and configured <see cref="SqliteConnection"/>.
    /// </returns>
    /// <remarks>
    /// The connection is configured with the following PRAGMA settings for optimal performance:
    /// <list type="bullet">
    ///   <item><description>journal_mode: As configured in options (default: WAL)</description></item>
    ///   <item><description>synchronous: As configured in options (default: NORMAL)</description></item>
    ///   <item><description>temp_store: MEMORY (temp tables in RAM)</description></item>
    ///   <item><description>mmap_size: 256MB (memory-mapped I/O)</description></item>
    ///   <item><description>cache_size: 64MB (page cache size)</description></item>
    /// </list>
    /// The caller is responsible for disposing the returned connection.
    /// </remarks>
    /// <exception cref="SqliteException">Thrown when the connection cannot be opened.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
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
    /// Configures an open connection with SQLite PRAGMA settings for optimal performance.
    /// </summary>
    /// <param name="connection">The open database connection to configure.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous configuration operation.</returns>
    private async Task ConfigureConnectionAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        // Set journal mode (WAL recommended for concurrent access)
        var journalMode = _options.JournalMode.ToString().ToUpperInvariant();
        await ExecutePragmaAsync(connection, string.Create(CultureInfo.InvariantCulture, $"journal_mode = {journalMode}"), cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, string.Create(CultureInfo.InvariantCulture, $"synchronous = {(int)_options.SynchronousMode}"), cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, "temp_store = MEMORY", cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, "mmap_size = 268435456", cancellationToken).ConfigureAwait(false);
        await ExecutePragmaAsync(connection, "cache_size = -64000", cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a SQLite PRAGMA statement on the specified connection.
    /// </summary>
    /// <param name="connection">The database connection on which to execute the PRAGMA.</param>
    /// <param name="pragma">The PRAGMA statement (without the "PRAGMA " prefix).</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
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
    /// Ensures the database schema (table and indexes) is initialized.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous schema initialization.</returns>
    /// <remarks>
    /// <para>
    /// This method is thread-safe and uses double-checked locking with a semaphore
    /// to ensure the schema is only initialized once, even when called concurrently.
    /// </para>
    /// <para>
    /// If <see cref="SQLiteSinkOptions.AutoCreateDatabase"/> is <c>false</c>, this method
    /// returns immediately without creating any schema.
    /// </para>
    /// <para>
    /// The method creates the database directory if it doesn't exist, creates the log table
    /// with all standard and custom columns, and creates indexes for improved query performance.
    /// </para>
    /// </remarks>
    /// <exception cref="SqliteException">Thrown when schema creation fails.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
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

            // Create directory if needed
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

    /// <summary>
    /// Ensures the directory for the database file exists, creating it if necessary.
    /// </summary>
    /// <remarks>
    /// This method is a no-op for in-memory databases (<c>:memory:</c>).
    /// </remarks>
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
    /// Creates the log table in the database if it doesn't already exist.
    /// </summary>
    /// <param name="connection">An open database connection.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous table creation.</returns>
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
    /// Builds the SQL CREATE TABLE statement for the log table.
    /// </summary>
    /// <returns>A complete CREATE TABLE IF NOT EXISTS SQL statement.</returns>
    /// <remarks>
    /// The generated table includes all standard columns defined in <see cref="Columns"/>
    /// plus any custom columns defined in <see cref="SQLiteSinkOptions.CustomColumns"/>.
    /// </remarks>
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
    /// Creates database indexes for improved query performance.
    /// </summary>
    /// <param name="connection">An open database connection.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous index creation.</returns>
    /// <remarks>
    /// The following indexes are created by default:
    /// <list type="bullet">
    ///   <item><description>Index on Timestamp for time-based queries</description></item>
    ///   <item><description>Index on Level for log level filtering</description></item>
    ///   <item><description>Composite index on Timestamp and Level for combined filtering</description></item>
    ///   <item><description>Individual indexes for custom columns where <see cref="CustomColumn.CreateIndex"/> is <c>true</c></description></item>
    /// </list>
    /// </remarks>
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
    /// Gets the current size of the database file in bytes.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the database size in bytes, or <c>0</c> for in-memory databases.
    /// </returns>
    /// <remarks>
    /// The size is calculated using SQLite's page_count and page_size PRAGMA values,
    /// providing an accurate measure of the actual database file size including any
    /// unused pages.
    /// </remarks>
    public async Task<long> GetDatabaseSizeAsync(CancellationToken cancellationToken = default)
    {
        if (_options.DatabasePath == ":memory:")
        {
            return 0;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // Query page count
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
    /// Gets the current number of log entries in the database.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// A task that represents the asynchronous operation. The task result contains
    /// the number of log entries in the configured table.
    /// </returns>
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
    /// Executes the VACUUM command to rebuild the database file and reclaim unused space.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A task representing the asynchronous VACUUM operation.</returns>
    /// <remarks>
    /// <para>
    /// VACUUM rebuilds the database file, repacking it into a minimal amount of disk space.
    /// This is useful after deleting large amounts of data to reduce file size.
    /// </para>
    /// <para>
    /// This operation is a no-op for in-memory databases.
    /// </para>
    /// <para>
    /// Note: VACUUM requires exclusive access to the database and may take significant
    /// time for large databases. It also temporarily doubles the disk space requirement.
    /// </para>
    /// </remarks>
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

    /// <summary>
    /// Releases all resources used by the <see cref="DatabaseManager"/>.
    /// </summary>
    /// <remarks>
    /// This method disposes the internal semaphore used for thread synchronization.
    /// Note that database connections are not pooled by this class; connection pooling
    /// is handled by the Microsoft.Data.Sqlite library.
    /// </remarks>
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
