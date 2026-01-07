# Serilog.Sinks.SQLite.Modern

[![NuGet](https://img.shields.io/nuget/v/Serilog.Sinks.SQLite.Modern.svg)](https://www.nuget.org/packages/Serilog.Sinks.SQLite.Modern)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%209.0-512BD4)](https://dotnet.microsoft.com/)

Eine moderne, hochperformante Serilog-Sink für SQLite-Datenbanken. Entwickelt für .NET 8+ mit vollständiger **AnyCPU**-Unterstützung.

## Features

- ✅ **AnyCPU-kompatibel** - Verwendet `Microsoft.Data.Sqlite` (kein natives SQLite)
- ✅ **.NET 8.0, .NET 9.0 & .NET 10.0** Unterstützung
- ✅ **Asynchrones Batching** - Optimale Performance durch Batch-Schreiben
- ✅ **Automatische Retention** - Nach Zeit, Anzahl oder Datenbankgröße
- ✅ **Custom Columns** - Strukturierte Daten in eigenen Spalten speichern
- ✅ **WAL-Modus** - Optimiert für hohe Schreiblast
- ✅ **Thread-sicher** - Vollständig für paralleles Logging geeignet
- ✅ **Konfigurierbar** - Umfangreiche Optionen für jeden Anwendungsfall

## Installation

```bash
dotnet add package Serilog.Sinks.SQLite.Modern
```

## Schnellstart

### Einfache Verwendung

```csharp
using Serilog;

var logger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/app.db")
    .CreateLogger();

logger.Information("Hello, SQLite!");
logger.Error(new Exception("Oops!"), "An error occurred");

// Wichtig: Logger am Ende freigeben
await Log.CloseAndFlushAsync();
```

### Erweiterte Konfiguration

```csharp
using Serilog;
using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;

var logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .Enrich.FromLogContext()
    .Enrich.WithThreadId()
    .WriteTo.SQLite("logs/app.db", options =>
    {
        // Tabellenname
        options.TableName = "ApplicationLogs";
        
        // Retention: Logs älter als 30 Tage löschen
        options.RetentionPeriod = TimeSpan.FromDays(30);
        
        // Retention: Maximal 100.000 Einträge behalten
        options.RetentionCount = 100_000;
        
        // Retention: Datenbank max. 100 MB
        options.MaxDatabaseSize = 100 * 1024 * 1024;
        
        // Performance-Tuning
        options.BatchSizeLimit = 200;
        options.BatchPeriod = TimeSpan.FromSeconds(1);
        options.QueueLimit = 50000;
        
        // SQLite-Optimierungen
        options.JournalMode = SQLiteJournalMode.Wal;
        options.SynchronousMode = SQLiteSynchronousMode.Normal;
        
        // Zeitstempel in UTC
        options.StoreTimestampInUtc = true;
        
        // Minimum Log-Level für diese Sink
        options.RestrictedToMinimumLevel = LogEventLevel.Information;
    })
    .CreateLogger();
```

### Custom Columns

Speichern Sie strukturierte Daten in eigenen Spalten für bessere Abfragen:

```csharp
var logger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/app.db", options =>
    {
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "UserId",
            DataType = "TEXT",
            PropertyName = "UserId",
            CreateIndex = true // Index für schnelle Suche
        });
        
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "RequestId",
            DataType = "TEXT",
            PropertyName = "RequestId"
        });
        
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "Duration",
            DataType = "REAL",
            PropertyName = "DurationMs"
        });
    })
    .CreateLogger();

// Verwendung
logger
    .ForContext("UserId", "user123")
    .ForContext("RequestId", Guid.NewGuid())
    .ForContext("DurationMs", 42.5)
    .Information("Request processed");
```

### Fehlerbehandlung

```csharp
var logger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/app.db", options =>
    {
        options.OnError = ex => 
        {
            Console.WriteLine($"SQLite Error: {ex.Message}");
            // Oder: Fallback-Logger verwenden
        };
        
        // Bei kritischen Fehlern Exception werfen
        options.ThrowOnError = false; // Standard: false
    })
    .CreateLogger();
```

## Datenbankschema

Die Sink erstellt automatisch folgende Tabelle:

| Spalte | Typ | Beschreibung |
|--------|-----|--------------|
| `Id` | INTEGER | Primärschlüssel (Auto-Increment) |
| `Timestamp` | TEXT | ISO 8601 Zeitstempel |
| `Level` | INTEGER | Log-Level (0-5) |
| `LevelName` | TEXT | Log-Level Name |
| `Message` | TEXT | Gerenderte Nachricht |
| `MessageTemplate` | TEXT | Original Message Template |
| `Exception` | TEXT | Exception Details (falls vorhanden) |
| `Properties` | TEXT | Properties als JSON |
| `SourceContext` | TEXT | Logger-Name / Quelle |
| `MachineName` | TEXT | Computername |
| `ThreadId` | INTEGER | Thread-ID |

Plus alle konfigurierten Custom Columns.

## Abfragen der Logs

```sql
-- Alle Fehler der letzten 24 Stunden
SELECT * FROM Logs 
WHERE Level >= 4 
AND Timestamp > datetime('now', '-1 day')
ORDER BY Timestamp DESC;

-- Logs nach UserId (wenn Custom Column konfiguriert)
SELECT * FROM Logs 
WHERE UserId = 'user123' 
ORDER BY Timestamp DESC 
LIMIT 100;

-- Aggregation nach Level
SELECT LevelName, COUNT(*) as Count 
FROM Logs 
GROUP BY Level;

-- Properties durchsuchen (JSON)
SELECT * FROM Logs 
WHERE json_extract(Properties, '$.RequestId') = 'abc123';
```

## Performance-Tipps

### 1. Batch-Größe optimieren

```csharp
options.BatchSizeLimit = 500;  // Für High-Volume
options.BatchPeriod = TimeSpan.FromMilliseconds(100);
```

### 2. WAL-Modus verwenden (Standard)

```csharp
options.JournalMode = SQLiteJournalMode.Wal;
```

### 3. Synchronous-Mode anpassen

```csharp
// Schneller, aber weniger sicher bei Stromausfall
options.SynchronousMode = SQLiteSynchronousMode.Normal;

// Oder für maximale Performance (nur wenn Datenverlust akzeptabel)
options.SynchronousMode = SQLiteSynchronousMode.Off;
```

### 4. Queue-Limit setzen

```csharp
// Verhindert Memory-Overflow bei Burst-Traffic
options.QueueLimit = 100000;
```

## Vergleich zu anderen SQLite Sinks

| Feature | Serilog.Sinks.SQLite.Modern | Serilog.Sinks.SQLite |
|---------|----------------------------|---------------------|
| AnyCPU Support | ✅ (Microsoft.Data.Sqlite) | ❌ (System.Data.SQLite) |
| .NET 8/9/10 | ✅ | ⚠️ (nur .NET 7) |
| Async Batching | ✅ | ✅ |
| Retention Policies | ✅ (Zeit, Anzahl, Größe) | ❌ |
| Custom Columns | ✅ | ❌ |
| WAL Mode | ✅ | ✅ |

## API-Referenz

### SQLiteSinkOptions

| Property | Typ | Standard | Beschreibung |
|----------|-----|----------|--------------|
| `DatabasePath` | string | "logs.db" | Pfad zur Datenbank |
| `TableName` | string | "Logs" | Tabellenname |
| `StoreTimestampInUtc` | bool | true | UTC oder Lokalzeit |
| `RestrictedToMinimumLevel` | LogEventLevel | Verbose | Minimum Level |
| `RetentionPeriod` | TimeSpan? | null | Max. Alter der Logs |
| `RetentionCount` | long? | null | Max. Anzahl Logs |
| `MaxDatabaseSize` | long? | null | Max. DB-Größe (Bytes) |
| `BatchSizeLimit` | int | 100 | Events pro Batch |
| `BatchPeriod` | TimeSpan | 2s | Batch-Intervall |
| `QueueLimit` | int? | 10000 | Max. Queue-Größe |
| `JournalMode` | SQLiteJournalMode | Wal | SQLite Journal Mode |
| `SynchronousMode` | SQLiteSynchronousMode | Normal | Sync Mode |

## Lizenz

Apache 2.0 - Siehe [LICENSE](LICENSE) für Details.

## Beitragen

Pull Requests sind willkommen! Bitte öffnen Sie zuerst ein Issue, um Änderungen zu diskutieren.

## Changelog

### 1.0.0

- Initial Release
- .NET 8.0, .NET 9.0 und .NET 10.0 Support
- AnyCPU-Kompatibilität mit Microsoft.Data.Sqlite
- Async Batching
- Retention Policies (Zeit, Anzahl, Größe)
- Custom Columns
- WAL Mode Support
