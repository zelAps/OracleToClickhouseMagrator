using Spectre.Console;
using Spectre.Console.Cli;
using Migrator.Core.Config;
using Migrator.Core.ClickHouse;
using Migrator.Core.Oracle;
using Migrator.Core.Models;
using System.Collections.Generic;

namespace Migrator.Cli.Commands;

/// <summary>
/// Команда создаёт в ClickHouse все таблицы, описанные в конфигурации.
/// </summary>
public sealed class CreateAllCommand : AsyncCommand<CommonSettings>
{
    /// <summary>
    /// Выполняет создание таблиц.
    /// </summary>
    /// <param name="ctx">Контекст выполнения команды.</param>
    /// <param name="s">Общие настройки, содержащие путь к конфигурации и флаг сухого прогона.</param>
    /// <returns>Код завершения операции.</returns>
    public override async Task<int> ExecuteAsync(CommandContext ctx, CommonSettings s)
    {
        // загружаем конфигурацию из указанного файла
        var cfg = await MigratorConfig.LoadAsync(s.ConfigPath);
        TypeMapper mapper = new();
        var reader = new OracleSchemaReader(cfg.Oracle.ConnectionString);
        var ddl = new ClickHouseDdlBuilder(cfg);

        // перебираем все таблицы из конфигурации
        foreach (var tblCfg in cfg.Tables)
        {
            // читаем схему таблицы из Oracle и строим SQL для ClickHouse
            var tbl = await reader.GetTableAsync(tblCfg, mapper.Map);
            var sql = ddl.BuildAll(tbl);

            if (s.DryRun)
            {
                AnsiConsole.Write(new Markup($"[grey]{Markup.Escape(sql)}[/]\n"));
            }
            else
            {
                await ExecClickHouseAsync(cfg, sql); // выполняем запрос
            }
        }

        return 0;
    }

    private static async Task ExecClickHouseAsync(MigratorConfig cfg, string sql)
    {
        var cs = $"Host=localhost;Database={cfg.ClickHouse.Database}";
        await using var ch = new ClickHouse.Client.ADO.ClickHouseConnection(cs);
        await ch.OpenAsync();
        await using var cmd = ch.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }
}
