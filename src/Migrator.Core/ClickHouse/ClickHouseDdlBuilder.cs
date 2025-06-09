using Migrator.Core.Config;
using Migrator.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Migrator.Core.ClickHouse;

/// <summary>
/// Генерирует DDL-скрипты для локальных и распределённых таблиц.
/// </summary>
public sealed class ClickHouseDdlBuilder(MigratorConfig cfg)
{
    private readonly MigratorConfig _cfg = cfg;

    /* ---------- публичный API ---------- */

    /// <summary>
    /// Формирует DDL для локальной таблицы ReplicatedMergeTree.
    /// </summary>
    /// <remarks>
    /// Генерируется секция <c>CREATE TABLE</c> со всеми колонками,
    /// путём сборки Zookeeper-пути и установкой выражений PARTITION BY
    /// и ORDER BY. Список колонок и выражения передаются через
    /// <see cref="TableDef"/>.
    /// </remarks>
    public string BuildLocal(TableDef t)
    {
        var sb = new StringBuilder();

        sb.AppendLine($"CREATE TABLE {_cfg.ClickHouse.Database}.{t.Target}_local");
        sb.AppendLine("(");
        sb.AppendLine(string.Join(",\n", t.Columns.Select(RenderColumn)));
        sb.AppendLine(")");
        sb.AppendLine("ENGINE = ReplicatedMergeTree(");
        sb.Append("    '");
        sb.Append(ZkPathHelper.Build(
            _cfg.ClickHouse.ZkPathPrefix,
            _cfg.ClickHouse.Cluster,
            _cfg.ClickHouse.Database,
            t.Target));
        sb.AppendLine("', '{replica}')");
        sb.AppendLine($"PARTITION BY {t.PartitionExpr}");
        sb.AppendLine($"ORDER BY ({string.Join(", ", BuildOrderBy(t))});");

        return sb.ToString();
    }

    /// <summary>
    /// Строит определение распределённой таблицы поверх локальной.
    /// </summary>
    /// <remarks>
    /// Если shardKey не задан ни в таблице, ни в конфигурации,
    /// используется первый столбец первичного ключа или случайное значение.
    /// </remarks>
    public string BuildDistributed(TableDef t)
    {
        var shardKey = t.ShardKey
                       ?? _cfg.ClickHouse.DistributedShardKey
                       ?? t.PrimaryKey.FirstOrDefault()
                       ?? "rand()";

        return $"""
        CREATE TABLE {_cfg.ClickHouse.Database}.{t.Target}
        AS {_cfg.ClickHouse.Database}.{t.Target}_local
        ENGINE = Distributed(
            '{_cfg.ClickHouse.Cluster}',
            '{_cfg.ClickHouse.Database}',
            '{t.Target}_local',
            {shardKey});
        """;
    }

    /* ---------- helpers ---------- */

    private static string RenderColumn(ColumnDef c)
        => $"    `{c.TargetName}` {c.ClickHouseType}";

    private static IEnumerable<string> BuildOrderBy(TableDef t)
        // сначала шардинг-ключ, затем PK (если есть)
        => (t.ShardKey is not null
                ? new[] { t.ShardKey }
                : Array.Empty<string>())
           .Concat(t.PrimaryKey);

    /* ---------- пакетный билд ---------- */

    /// <summary>
    /// Формирует DDL обоих уровней (локальной и распределённой) одной строкой.
    /// </summary>
    public string BuildAll(TableDef t)
        => $"{BuildLocal(t)}\n\n{BuildDistributed(t)}";
}
