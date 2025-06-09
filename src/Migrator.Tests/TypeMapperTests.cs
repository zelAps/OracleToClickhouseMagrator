using Migrator.Core.ClickHouse;
using Migrator.Core.Models;
using Xunit;

namespace Migrator.Tests;

public class TypeMapperTests
{
    [Fact]
    public void Map_NumberWithoutScale_ReturnsInt32()
    {
        var mapper = new TypeMapper();
        var col = new ColumnDef
        {
            SourceName = "A",
            SourceType = "NUMBER",
            Precision = 9,
            Scale = null,
            Nullable = false,
            TargetName = "a",
            ClickHouseType = string.Empty
        };

        mapper.Map(col);

        Assert.Equal("Int32", col.ClickHouseType);
    }

    [Fact]
    public void Map_NumberWithoutScale_PrecisionBetween10And18_ReturnsInt64()
    {
        var mapper = new TypeMapper();
        var col = new ColumnDef
        {
            SourceName = "B",
            SourceType = "NUMBER",
            Precision = 15,
            Scale = 0,
            Nullable = true,
            TargetName = "b",
            ClickHouseType = string.Empty
        };

        mapper.Map(col);

        Assert.Equal("Nullable(Int64)", col.ClickHouseType);
    }

    [Fact]
    public void Map_NumberWithScale_ReturnsDecimal()
    {
        var mapper = new TypeMapper();
        var col = new ColumnDef
        {
            SourceName = "C",
            SourceType = "NUMBER",
            Precision = 10,
            Scale = 2,
            Nullable = false,
            TargetName = "c",
            ClickHouseType = string.Empty
        };

        mapper.Map(col);

        Assert.Equal("Decimal(10,2)", col.ClickHouseType);
    }

    [Fact]
    public void Map_Varchar2Nullable_ReturnsNullableString()
    {
        var mapper = new TypeMapper();
        var col = new ColumnDef
        {
            SourceName = "D",
            SourceType = "VARCHAR2",
            Nullable = true,
            Precision = null,
            Scale = null,
            TargetName = "d",
            ClickHouseType = string.Empty
        };

        mapper.Map(col);

        Assert.Equal("Nullable(String)", col.ClickHouseType);
    }

    [Fact]
    public void Map_CharNotNullable_ReturnsString()
    {
        var mapper = new TypeMapper();
        var col = new ColumnDef
        {
            SourceName = "E",
            SourceType = "CHAR",
            Nullable = false,
            Precision = null,
            Scale = null,
            TargetName = "e",
            ClickHouseType = string.Empty
        };

        mapper.Map(col);

        Assert.Equal("String", col.ClickHouseType);
    }
}

