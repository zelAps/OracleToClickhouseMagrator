using Spectre.Console.Cli;
using Migrator.Core.Config;
using Migrator.Core.ClickHouse;
using Migrator.Core.Oracle;
using Migrator.DataPump;
using System.Threading.Tasks;
using System.Linq;

namespace Migrator.Cli.Commands;

/// <summary>
/// Команда мигрирует выбранные таблицы из Oracle в ClickHouse.
/// </summary>
public sealed class MigrateTablesCommand : AsyncCommand<TableNamesSettings>
{
    /// <summary>
    /// Запускает перенос указанных таблиц.
    /// </summary>
    /// <param name="ctx">Контекст исполнения.</param>
    /// <param name="s">Параметры со списком таблиц и путём к конфигурации.</param>
    /// <returns>Код завершения.</returns>
    public override async Task<int> ExecuteAsync(CommandContext ctx, TableNamesSettings s)
    {
        // получаем конфигурацию и список требуемых таблиц
        var cfg = await MigratorConfig.LoadAsync(s.ConfigPath);
        var want = s.TableNames.Select(t => t.ToUpperInvariant()).ToHashSet();

        var mapper = new TypeMapper();
        var reader = new OracleSchemaReader(cfg.Oracle.ConnectionString);

        // фильтруем таблицы по списку аргументов
        var toMove = cfg.Tables
                        .Where(t => want.Contains(t.Source.ToUpperInvariant()))
                        .ToList();

        if (toMove.Count == 0)
        {
            Console.WriteLine("⛔ Нечего мигрировать — список таблиц пустой.");
            return -1;
        }

        // мигрируем каждую выбранную таблицу
        foreach (var t in toMove)
        {
            // читаем схему и инициализируем мигратор
            var tbl = await reader.GetTableAsync(t, mapper.Map);
            var pump = new DataMigrator(
                cfg.Oracle.ConnectionString,
                $"Host=localhost;Database={cfg.ClickHouse.Database}",
                tbl);

            await pump.RunAsync(t.Where ?? string.Empty); // запускаем перенос
        }

        return 0;
    }
}
