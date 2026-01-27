# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Raycoon.Serilog.Sinks.SQLite is a high-performance Serilog sink for SQLite databases. Key differentiators:
- Uses `Microsoft.Data.Sqlite` for AnyCPU compatibility (not System.Data.SQLite)
- Targets .NET 8.0, 9.0, and 10.0
- Async batching via Serilog.Sinks.PeriodicBatching
- Retention policies (time, count, size-based)
- Custom columns support

## Build Commands

```bash
dotnet restore           # Restore dependencies
dotnet build             # Build all projects
dotnet build -c Release  # Release build
dotnet test              # Run all tests
dotnet test --filter "FullyQualifiedName~TestMethodName"  # Run single test
dotnet pack src/Raycoon.Serilog.Sinks.SQLite/ -c Release   # Create NuGet package
dotnet run --project samples/SampleConsoleApp/SampleConsoleApp.csproj  # Run sample
```

## Architecture

```
src/Raycoon.Serilog.Sinks.SQLite/
├── Extensions/SQLiteLoggerConfigurationExtensions.cs  # Public fluent API (.WriteTo.SQLite())
├── Options/SQLiteSinkOptions.cs                       # Configuration + CustomColumn class
├── Sinks/SQLiteSink.cs                                # Core sink (IBatchedLogEventSink)
└── Internal/
    ├── DatabaseManager.cs      # Connection pooling, schema, PRAGMA settings
    ├── LogEventBatchWriter.cs  # Batch INSERT with transactions
    └── RetentionManager.cs     # Background cleanup timer
```

**Key patterns:**
- Batching via Serilog.Sinks.PeriodicBatching wrapping the sink
- Schema created lazily on first write (SemaphoreSlim for thread safety)
- All I/O is async with `ConfigureAwait(false)`
- Connection pooling handled by Microsoft.Data.Sqlite

## Testing

- Framework: xUnit with FluentAssertions and NSubstitute
- Code coverage via Coverlet
- Tests run against all target frameworks (net8.0, net9.0, net10.0)
- Test naming convention: underscores allowed (CA1707 suppressed)
- Internal classes accessible via `InternalsVisibleTo`

## Code Style

- File-scoped namespaces (`csharp_style_namespace_declarations = file_scoped`)
- Private fields prefixed with underscore (`_fieldName`)
- `var` preferred throughout (not just when type is apparent)
- Nullable reference types enabled
- Suppressed analyzer warnings (library-specific):
  - CA2007: ConfigureAwait (library uses `ConfigureAwait(false)` where needed)
  - CA1031: Catch generic exceptions (logging sink must be robust)
  - CA2100: SQL injection (SQL built from internal options, not user input)
- Code-Documentation in english

## License

Apache-2.0
