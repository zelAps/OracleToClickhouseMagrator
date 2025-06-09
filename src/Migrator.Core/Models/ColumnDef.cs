namespace Migrator.Core.Models;

/// <summary>
/// Описание колонки, полученное из Oracle
/// и дополненное сведениями для создания таблицы в ClickHouse.
/// </summary>
/// <remarks>
/// Используется как промежуточная модель между чтением схемы и
/// генерацией DDL: в ней хранится исходное имя и тип колонки, а также
/// результат маппинга и переименования.
/// </remarks>
public sealed class ColumnDef
{
    /* ----- исходные данные Oracle ----- */
    public required string SourceName { get; init; }            // EMP_ID
    public required string SourceType { get; init; }            // NUMBER
    public int? Precision { get; init; }                        // 10
    public int? Scale { get; init; }                        // 0
    public bool Nullable { get; init; }
    public int? DataLength { get; init; }                        // для VARCHAR2
    public string? Default { get; init; }

    /* ----- целевые данные ClickHouse (заполняются далее) ----- */
    public required string TargetName { get; set; }             // id
    public required string ClickHouseType { get; set; }         // UInt32  | Nullable(String)

    public override string ToString() =>
        $"{SourceName} ({SourceType}) -> {TargetName} {ClickHouseType}";
}
