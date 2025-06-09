using Spectre.Console.Cli;

namespace Migrator.Cli;

/// <summary>
/// Общие настройки, применимые ко всем командам мигратора.
/// </summary>
public class CommonSettings : CommandSettings
{
    /// <summary>
    /// Путь к YAML‑файлу конфигурации.
    /// Значение по умолчанию — <c>config.yaml</c>.
    /// </summary>
    [CommandOption("-c|--config <FILE>")]
    public string ConfigPath { get; init; } = "config.yaml";

    /// <summary>
    /// Только вывести сформированные запросы без их выполнения.
    /// </summary>
    [CommandOption("--dry-run")]
    public bool DryRun { get; init; }

    /// <summary>
    /// Печатать дополнительную диагностическую информацию.
    /// </summary>
    [CommandOption("-v|--verbose")]
    public bool Verbose { get; init; }
}
