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
/// Координатор процесса миграции: читает данные из Oracle и
/// записывает их в ClickHouse, разбивая поток на блоки.
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

    /// <summary>
    /// Читает данные из Oracle и пакетами отправляет их в ClickHouse.
    /// </summary>
    public async Task<TransferStats> RunAsync(string? where, CancellationToken ct = default)
    {
        var stats = new TransferStats();

        await using var orclConn = new OracleConnection(_oracleCs);
        await orclConn.OpenAsync(ct);                 // подключение к Oracle

        // формируем SQL с нужными колонками и возможным фильтром
        var select = $"SELECT {string.Join(",", _tbl.Columns.Select(c => c.SourceName))} " +
                     $"FROM {_tbl.Source}" +
                     (string.IsNullOrWhiteSpace(where) ? "" : $" WHERE {where}");

        _log.Information("⏩ Start {Table} ({Where})", _tbl.Source, where ?? "full");

        // создаём потоковый читатель и bulk‑writer для работы блоками
        await using var reader = new OracleStreamReader(orclConn, select, batchSize: _batchSize);
        await using var writer = new ClickHouseBulkWriter(_clickCs,
            $"{_tbl.Target}_local", _batchSize);

        // читаем Oracle по частям и отправляем каждую порцию в ClickHouse
        await foreach (var block in reader.ReadAsync(ct))
        {
            try
            {
                await writer.WriteAsync(block.Span.ToArray(), ct);
                stats.Add(block.Length, 0);           // bytes ≈ неточные, можно оценить
            }
            catch (Exception ex)
            {
                // при ошибке записываем событие и пропускаем блок
                _log.Error(ex, "Insert failed, block skipped");
                stats.Fail();
            }
        }

        _log.Information("✅ {Rows} rows → {Table} in {Elapsed}",
            stats.Rows, _tbl.Target, stats.Elapsed);

        return stats;
    }
}
