using Oracle.ManagedDataAccess.Client;
using ClickHouse.Client.ADO;
using Migrator.Core.Models;
using Migrator.DataPump;
using Migrator.Core.ClickHouse;
using Serilog;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Migrator.DataPump;

/// <summary>
/// Оркестрация: читает → маппит → пишет блоками.
/// </summary>
public sealed class DataMigrator(
    string oracleCs,
    string clickhouseCs,
    TableDef table,
    int batchSize = 100_000)
{
    private readonly string _oracleCs = oracleCs;
    private readonly string _clickCs = clickhouseCs;
    private readonly TableDef _tbl = table;
    private readonly int _batchSize = batchSize;

    private readonly ILogger _log = Log.ForContext<DataMigrator>();

    public async Task<TransferStats> RunAsync(string? where, CancellationToken ct = default)
    {
        var stats = new TransferStats();

        await using var orclConn = new OracleConnection(_oracleCs);
        await orclConn.OpenAsync(ct);

        var select = $"SELECT {string.Join(",", _tbl.Columns.Select(c => c.SourceName))} " +
                     $"FROM {_tbl.Source}" +
                     (string.IsNullOrWhiteSpace(where) ? "" : $" WHERE {where}");

        _log.Information("⏩ Start {Table} ({Where})", _tbl.Source, where ?? "full");

        await using var reader = new OracleStreamReader(orclConn, select, batchSize: _batchSize);
        await using var writer = new ClickHouseBulkWriter(_clickCs,
            $"{_tbl.Target}_local", _batchSize);

        await foreach (var block in reader.ReadAsync(ct))
        {
            try
            {
                await writer.WriteAsync(block.Span.ToArray(), ct);
                stats.Add(block.Length, 0);           // bytes ≈ неточные, можно оценить
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Insert failed, block skipped");
                stats.Fail();
            }
        }

        _log.Information("✅ {Rows} rows → {Table} in {Elapsed}",
            stats.Rows, _tbl.Target, stats.Elapsed);

        return stats;
    }
}
