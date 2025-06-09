using Serilog;
using Serilog.Events;

namespace Migrator.DataPump;

/// <summary>
/// Настройка Serilog и предоставление логгера с контекстом типа.
/// </summary>
public static class Log
{
    public static ILogger ForContext<T>() => _logger.ForContext<T>();

    private static readonly ILogger _logger = new LoggerConfiguration()
        .MinimumLevel.Debug()
        .WriteTo.File("migrator-.log",
            rollingInterval: RollingInterval.Day,
            restrictedToMinimumLevel: LogEventLevel.Information)
        .CreateLogger();
}
