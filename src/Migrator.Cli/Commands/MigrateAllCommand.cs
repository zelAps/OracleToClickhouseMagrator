using Spectre.Console.Cli;
using Migrator.Core.ClickHouse;
using Migrator.Core.Oracle;
using Migrator.Core.Config;
using Migrator.DataPump;
using System.Collections.Generic;

namespace Migrator.Cli.Commands;

/// <summary>
/// Команда переносит данные всех таблиц из Oracle в ClickHouse.
/// </summary>
public sealed class MigrateAllCommand : AsyncCommand<CommonSettings>
{
    /// <summary>
    /// Запускает миграцию всех таблиц.
    /// </summary>
    /// <param name="ctx">Контекст выполнения.</param>
    /// <param name="s">Общие настройки с конфигурацией и флагами.</param>
    /// <returns>Код завершения.</returns>
    public override async Task<int> ExecuteAsync(CommandContext ctx, CommonSettings s)
    {
        // читаем настройки миграции
        var cfg = await MigratorConfig.LoadAsync(s.ConfigPath);
        var mapper = new TypeMapper();
        var reader = new OracleSchemaReader(cfg.Oracle.ConnectionString);

        // проходим по всем таблицам из конфигурации
        foreach (var t in cfg.Tables)
        {
            // читаем схему и подготавливаем мигратор
            var tbl = await reader.GetTableAsync(t, mapper.Map);
            var pump = new DataMigrator(
                cfg.Oracle.ConnectionString,
                $"Host=localhost;Database={cfg.ClickHouse.Database}",
                tbl);

            await pump.RunAsync(t.Where ?? string.Empty); // запускаем миграцию
        }

        return 0;
    }
}
