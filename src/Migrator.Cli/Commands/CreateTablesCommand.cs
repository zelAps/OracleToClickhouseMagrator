using Spectre.Console;
using Spectre.Console.Cli;
using Migrator.Core.Config;
using Migrator.Core.ClickHouse;
using Migrator.Core.Oracle;
using System.Threading.Tasks;

namespace Migrator.Cli.Commands;

public sealed class CreateTablesCommand : AsyncCommand<TableNamesSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext ctx, TableNamesSettings s)
    {
        var cfg = await MigratorConfig.LoadAsync(s.ConfigPath);
        var want = s.TableNames.Select(t => t.ToUpperInvariant()).ToHashSet();
        var mapper = new TypeMapper();
        var reader = new OracleSchemaReader(cfg.Oracle.ConnectionString);
        var ddlBldr = new ClickHouseDdlBuilder(cfg);

        var targets = cfg.Tables
                         .Where(t => want.Contains(t.Source.ToUpperInvariant()))
                         .ToList();

        if (targets.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]⛔ Нечего создавать — список таблиц пустой.[/]");
            return -1;
        }

        foreach (var t in targets)
        {
            var tbl = await reader.GetTableAsync(t, mapper.Map);
            var sql = ddlBldr.BuildAll(tbl);

            if (s.DryRun)
                AnsiConsole.MarkupLine($"[blue]{Markup.Escape(sql)}[/]");
            else
                await ExecClickHouseAsync(cfg, sql);
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
