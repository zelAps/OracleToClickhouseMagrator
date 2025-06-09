using Spectre.Console;
using Spectre.Console.Cli;
using Migrator.Core.Config;
using Migrator.Core.ClickHouse;
using Migrator.Core.Oracle;
using System.Threading.Tasks;

namespace Migrator.Cli.Commands;

/// <summary>
/// Команда создаёт в ClickHouse выбранные таблицы, переданные через аргументы.
/// </summary>
public sealed class CreateTablesCommand : AsyncCommand<TableNamesSettings>
{
    /// <summary>
    /// Выполняет создание перечисленных таблиц.
    /// </summary>
    /// <param name="ctx">Контекст выполнения команды.</param>
    /// <param name="s">Настройки с путём к конфигурации и списком имён таблиц.</param>
    /// <returns>Код завершения операции.</returns>
    public override async Task<int> ExecuteAsync(CommandContext ctx, TableNamesSettings s)
    {
        // загружаем конфигурацию и список нужных таблиц
        var cfg = await MigratorConfig.LoadAsync(s.ConfigPath);
        var want = s.TableNames.Select(t => t.ToUpperInvariant()).ToHashSet();
        var mapper = new TypeMapper();
        var reader = new OracleSchemaReader(cfg.Oracle.ConnectionString);
        var ddlBldr = new ClickHouseDdlBuilder(cfg);

        // выбираем из конфигурации только нужные таблицы
        var targets = cfg.Tables
                         .Where(t => want.Contains(t.Source.ToUpperInvariant()))
                         .ToList();

        if (targets.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]⛔ Нечего создавать — список таблиц пустой.[/]");
            return -1;
        }

        // создаём каждую выбранную таблицу
        foreach (var t in targets)
        {
            // читаем схему и формируем SQL
            var tbl = await reader.GetTableAsync(t, mapper.Map);
            var sql = ddlBldr.BuildAll(tbl);

            if (s.DryRun)
            {
                AnsiConsole.MarkupLine($"[blue]{Markup.Escape(sql)}[/]");
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
