using Microsoft.EntityFrameworkCore.Storage;
using System.Data;

namespace BasicSQL.EntityFramework.Storage;

/// <summary>
/// BasicSQL implementation of type mapping source.
/// </summary>
public class BasicSqlTypeMappingSource : RelationalTypeMappingSource
{
    /// <summary>
    /// Initializes a new instance of the <see cref="BasicSqlTypeMappingSource"/> class.
    /// </summary>
    /// <param name="dependencies">The dependencies.</param>
    /// <param name="relationalDependencies">The relational dependencies.</param>
    public BasicSqlTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    /// <summary>
    /// Gets the mapping for a given type.
    /// </summary>
    /// <param name="type">The .NET type.</param>
    /// <returns>The type mapping or null if not found.</returns>
    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;
        
        if (clrType != null)
        {
            // Handle basic types
            if (clrType == typeof(int) || clrType == typeof(int?))
                return new IntTypeMapping("INTEGER", DbType.Int32);
            
            if (clrType == typeof(long) || clrType == typeof(long?))
                return new LongTypeMapping("BIGINT", DbType.Int64);
            
            if (clrType == typeof(string))
                return new StringTypeMapping("TEXT", DbType.String);
            
            if (clrType == typeof(bool) || clrType == typeof(bool?))
                return new BoolTypeMapping("BOOLEAN", DbType.Boolean);
            
            if (clrType == typeof(double) || clrType == typeof(double?))
                return new DoubleTypeMapping("REAL", DbType.Double);
            
            if (clrType == typeof(float) || clrType == typeof(float?))
                return new FloatTypeMapping("REAL", DbType.Single);
            
            if (clrType == typeof(decimal) || clrType == typeof(decimal?))
                return new DecimalTypeMapping("DECIMAL", DbType.Decimal);
            
            if (clrType == typeof(DateTime) || clrType == typeof(DateTime?))
                return new DateTimeTypeMapping("DATETIME", DbType.DateTime);
            
            if (clrType == typeof(Guid) || clrType == typeof(Guid?))
                return new GuidTypeMapping("TEXT", DbType.String);
            
            if (clrType == typeof(byte[]))
                return new ByteArrayTypeMapping("BLOB", DbType.Binary);
        }

        return base.FindMapping(mappingInfo);
    }
}

/// <summary>
/// Basic type mapping implementations for BasicSQL
/// </summary>
public class IntTypeMapping : RelationalTypeMapping
{
    public IntTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(int), dbType)
    {
    }

    protected IntTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new IntTypeMapping(parameters);
}

public class LongTypeMapping : RelationalTypeMapping
{
    public LongTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(long), dbType)
    {
    }

    protected LongTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new LongTypeMapping(parameters);
}

public class StringTypeMapping : RelationalTypeMapping
{
    public StringTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(string), dbType)
    {
    }

    protected StringTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new StringTypeMapping(parameters);
}

public class BoolTypeMapping : RelationalTypeMapping
{
    public BoolTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(bool), dbType)
    {
    }

    protected BoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new BoolTypeMapping(parameters);
}

public class DoubleTypeMapping : RelationalTypeMapping
{
    public DoubleTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(double), dbType)
    {
    }

    protected DoubleTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DoubleTypeMapping(parameters);
}

public class FloatTypeMapping : RelationalTypeMapping
{
    public FloatTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(float), dbType)
    {
    }

    protected FloatTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new FloatTypeMapping(parameters);
}

public class DecimalTypeMapping : RelationalTypeMapping
{
    public DecimalTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(decimal), dbType)
    {
    }

    protected DecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecimalTypeMapping(parameters);
}

public class DateTimeTypeMapping : RelationalTypeMapping
{
    public DateTimeTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(DateTime), dbType)
    {
    }

    protected DateTimeTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DateTimeTypeMapping(parameters);
}

public class GuidTypeMapping : RelationalTypeMapping
{
    public GuidTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(Guid), dbType)
    {
    }

    protected GuidTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new GuidTypeMapping(parameters);
}

public class ByteArrayTypeMapping : RelationalTypeMapping
{
    public ByteArrayTypeMapping(string storeType, DbType dbType)
        : base(storeType, typeof(byte[]), dbType)
    {
    }

    protected ByteArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new ByteArrayTypeMapping(parameters);
}
