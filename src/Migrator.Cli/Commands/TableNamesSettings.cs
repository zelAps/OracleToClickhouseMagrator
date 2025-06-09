using Spectre.Console.Cli;

namespace Migrator.Cli.Commands;

/// <summary>
/// Общие параметры + позиционный список имён таблиц.
/// Пример вызова: `oracle2ch create tables EMP DEPT`
/// </summary>
public sealed class TableNamesSettings : CommonSettings
{
    /// <summary>
    /// Список таблиц, над которыми выполняется операция.
    /// </summary>
    [CommandArgument(0, "[TABLES]")]
    public string[] TableNames { get; init; } = [];
}
