// Copyright (c) 2025 Your Company. All rights reserved.
// Licensed under the Apache License, Version 2.0.

using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Events;
using Serilog.Sinks.SQLite.Modern.Options;
using System.Globalization;

Console.WriteLine("=== Serilog SQLite Sink Demo ===\n");

// Beispiel 1: Einfache Konfiguration
Console.WriteLine("Beispiel 1: Einfache Konfiguration");
Console.WriteLine(new string('-', 40));

await using (var simpleLogger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .WriteTo.Console(formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/simple.db")
    .CreateLogger())
{
    simpleLogger.Information("Dies ist eine einfache Log-Nachricht");
    simpleLogger.Debug("Debug-Information: Wert = {Value}", 42);
    simpleLogger.Warning("Eine Warnung mit Zeitstempel");
    
    await Task.Delay(500);
}

Console.WriteLine("✓ Einfache Logs wurden nach 'logs/simple.db' geschrieben\n");

// Beispiel 2: Erweiterte Konfiguration
Console.WriteLine("Beispiel 2: Erweiterte Konfiguration");
Console.WriteLine(new string('-', 40));

await using (var advancedLogger = new LoggerConfiguration()
    .MinimumLevel.Verbose()
    .Enrich.FromLogContext()
    .Enrich.WithThreadId()
    .Enrich.WithMachineName()
    .WriteTo.Console(
        outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}",
        formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/advanced.db", options =>
    {
        options.TableName = "ApplicationLogs";
        options.RetentionPeriod = TimeSpan.FromDays(7);
        options.RetentionCount = 10000;
        options.MaxDatabaseSize = 50 * 1024 * 1024; // 50 MB
        options.BatchSizeLimit = 50;
        options.BatchPeriod = TimeSpan.FromMilliseconds(500);
        options.JournalMode = SQLiteJournalMode.Wal;
        options.SynchronousMode = SQLiteSynchronousMode.Normal;
        
        // Custom Columns für strukturierte Daten
        options.CustomColumns.Add(new CustomColumn
        {
            ColumnName = "UserId",
            DataType = "TEXT",
            PropertyName = "UserId",
            CreateIndex = true
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
            PropertyName = "Duration"
        });
        
        // Error Callback
        options.OnError = ex => Console.WriteLine($"SQLite Fehler: {ex.Message}");
    })
    .CreateLogger())
{
    // Simuliere verschiedene Log-Szenarien
    var random = new Random();
    var userIds = new[] { "user1", "user2", "user3", "admin" };
    var actions = new[] { "Login", "Logout", "ViewPage", "UpdateProfile", "Purchase" };

    for (var i = 0; i < 20; i++)
    {
        var userId = userIds[random.Next(userIds.Length)];
        var action = actions[random.Next(actions.Length)];
        var duration = random.NextDouble() * 1000;
        var requestId = Guid.NewGuid().ToString("N")[..8];

        using (Serilog.Context.LogContext.PushProperty("UserId", userId))
        using (Serilog.Context.LogContext.PushProperty("RequestId", requestId))
        using (Serilog.Context.LogContext.PushProperty("Duration", duration))
        {
            if (random.NextDouble() < 0.1) // 10% Fehler
            {
                advancedLogger.Error(
                    new InvalidOperationException($"Aktion '{action}' fehlgeschlagen"),
                    "Fehler bei Aktion {Action} für Benutzer {UserId}",
                    action, userId);
            }
            else if (random.NextDouble() < 0.2) // 20% Warnungen
            {
                advancedLogger.Warning(
                    "Langsame Aktion {Action} - Dauer: {Duration:F2}ms",
                    action, duration);
            }
            else
            {
                advancedLogger.Information(
                    "Benutzer {UserId} führte Aktion {Action} aus - Dauer: {Duration:F2}ms",
                    userId, action, duration);
            }
        }
        
        await Task.Delay(50);
    }
    
    await Task.Delay(1000);
}

Console.WriteLine("✓ Erweiterte Logs wurden nach 'logs/advanced.db' geschrieben\n");

// Beispiel 3: Exception Logging
Console.WriteLine("Beispiel 3: Exception Logging");
Console.WriteLine(new string('-', 40));

await using (var exceptionLogger = new LoggerConfiguration()
    .WriteTo.Console(formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/exceptions.db", options =>
    {
        options.TableName = "Exceptions";
        options.StoreExceptionDetails = true;
        options.MaxExceptionLength = 8000;
    })
    .CreateLogger())
{
    try
    {
        await SimulateComplexException();
    }
    catch (Exception ex)
    {
        exceptionLogger.Error(ex, "Ein komplexer Fehler ist aufgetreten");
    }
    
    await Task.Delay(500);
}

Console.WriteLine("✓ Exception-Logs wurden nach 'logs/exceptions.db' geschrieben\n");

// Beispiel 4: High-Volume Logging
Console.WriteLine("Beispiel 4: High-Volume Logging (Performance Test)");
Console.WriteLine(new string('-', 40));

var stopwatch = System.Diagnostics.Stopwatch.StartNew();
const int messageCount = 5000;

await using (var highVolumeLogger = new LoggerConfiguration()
    .WriteTo.SQLite("logs/highvolume.db", options =>
    {
        options.BatchSizeLimit = 500;
        options.BatchPeriod = TimeSpan.FromMilliseconds(100);
        options.QueueLimit = 50000;
    })
    .CreateLogger())
{
    // Parallel logging
    await Parallel.ForEachAsync(
        Enumerable.Range(0, messageCount),
        new ParallelOptions { MaxDegreeOfParallelism = 10 },
        async (i, ct) =>
        {
            highVolumeLogger.Information(
                "High-Volume Message {MessageId}: Data={Data}, Timestamp={Timestamp}",
                i, 
                Guid.NewGuid(),
                DateTime.UtcNow);
            
            if (i % 1000 == 0)
            {
                await Task.Yield();
            }
        });
    
    await Task.Delay(2000); // Warten auf Flush
}

stopwatch.Stop();
Console.WriteLine($"✓ {messageCount:N0} Nachrichten in {stopwatch.ElapsedMilliseconds}ms geschrieben");
Console.WriteLine($"  Durchsatz: {messageCount / stopwatch.Elapsed.TotalSeconds:N0} msgs/sec\n");

// Beispiel 5: Konfiguration über JSON
Console.WriteLine("Beispiel 5: Konfiguration über JSON");
Console.WriteLine(new string('-', 40));

// Erstelle eine temporäre appsettings.json
var appSettingsContent = """
{
  "Serilog": {
    "MinimumLevel": {
      "Default": "Information",
      "Override": {
        "Microsoft": "Warning",
        "System": "Warning"
      }
    }
  }
}
""";

await File.WriteAllTextAsync("appsettings.json", appSettingsContent);

var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .Build();

await using (var configLogger = new LoggerConfiguration()
    .ReadFrom.Configuration(configuration)
    .WriteTo.Console(formatProvider: CultureInfo.InvariantCulture)
    .WriteTo.SQLite("logs/configured.db", options =>
    {
        options.TableName = "ConfiguredLogs";
    })
    .CreateLogger())
{
    configLogger.Information("Log aus JSON-konfiguriertem Logger");
    configLogger.Debug("Diese Debug-Nachricht wird gefiltert (MinLevel=Information)");
    
    await Task.Delay(500);
}

Console.WriteLine("✓ JSON-konfigurierte Logs wurden geschrieben\n");

// Zusammenfassung
Console.WriteLine("=== Zusammenfassung ===");
Console.WriteLine(new string('=', 40));

var logFiles = Directory.GetFiles("logs", "*.db")
    .Select(f => new FileInfo(f))
    .OrderBy(f => f.Name);

foreach (var file in logFiles)
{
    Console.WriteLine($"  {file.Name,-25} {file.Length / 1024.0:F1} KB");
}

Console.WriteLine("\n✓ Demo abgeschlossen!");

// Hilfsmethode für komplexe Exception
static async Task SimulateComplexException()
{
    try
    {
        await Task.Run(() =>
        {
            try
            {
                throw new InvalidOperationException("Innerer Fehler in Task");
            }
            catch (Exception inner)
            {
                throw new AggregateException("Mehrere Fehler aufgetreten", 
                    inner,
                    new ArgumentException("Ungültiges Argument"),
                    new TimeoutException("Operation hat zu lange gedauert"));
            }
        });
    }
    catch (Exception ex)
    {
        throw new ApplicationException("Anwendungsfehler mit verschachtelten Exceptions", ex);
    }
}
