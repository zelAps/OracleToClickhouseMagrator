using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using ClickHouse.Client.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Migrator.DataPump;

/// <summary>
/// Пакетно вставляет строки в ClickHouse через HTTP-binary протокол.
/// </summary>
public sealed class ClickHouseBulkWriter : IAsyncDisposable
{
    private readonly ClickHouseConnection _conn;
    private readonly ClickHouseBulkCopy _bulk;
    private readonly string _table;
    private readonly int _batchSize;

    public ClickHouseBulkWriter(
        string connectionString,
        string table,
        int batchSize = 100_000)
    {
        _conn = new ClickHouseConnection(connectionString);
        _conn.Open();                         // sync open ok

        _table = table;
        _batchSize = batchSize;

        _bulk = new ClickHouseBulkCopy(_conn)
        {
            DestinationTableName = table,
            BatchSize = batchSize,
            MaxDegreeOfParallelism = Environment.ProcessorCount
        };
    }

    /// <summary>Отправляет один блок строк.</summary>
    public async Task WriteAsync(IEnumerable<object?[]> rows, CancellationToken ct = default)
    {
        // ClickHouseBulkCopy принимает DataTable, поэтому переносим массивы
        var tbl = new DataTable();
        var firstRow = rows.First();
        for (var i = 0; i < firstRow.Length; i++)
            tbl.Columns.Add(new DataColumn($"c{i}", typeof(object)));

        foreach (var r in rows)
            tbl.Rows.Add(r);

        // отправка подготовленной таблицы одним запросом
        await _bulk.WriteToServerAsync(tbl, ct);
    }

    public async ValueTask DisposeAsync()
    {
        await _conn.DisposeAsync();
    }
}
