using System;

namespace BasicSQL.Models
{
    /// <summary>
    /// Supported data types in the SQL engine
    /// </summary>
    public enum DataType
    {
        Integer,
        Long,
        Text,
        Real
    }

    /// <summary>
    /// Represents a table column with its properties
    /// </summary>
    public class Column
    {
        public string Name { get; set; } = string.Empty;
        public DataType DataType { get; set; }
        public bool IsNullable { get; set; } = true;
        public bool IsPrimaryKey { get; set; } = false;
        public bool IsAutoIncrement { get; set; } = false;

        public Column() { }

        public Column(string name, DataType dataType, bool isNullable = true, bool isPrimaryKey = false, bool isAutoIncrement = false)
        {
            Name = name;
            DataType = dataType;
            IsNullable = isNullable;
            IsPrimaryKey = isPrimaryKey;
            IsAutoIncrement = isAutoIncrement;
            
            // Primary key columns cannot be nullable
            if (isPrimaryKey)
                IsNullable = false;
                
            // Auto-increment columns cannot be nullable and must be Integer or Long
            if (isAutoIncrement)
            {
                IsNullable = false;
                if (dataType != DataType.Integer && dataType != DataType.Long)
                    throw new ArgumentException("Auto-increment columns must be INTEGER or LONG");
            }
        }

        /// <summary>
        /// Validates if a value is compatible with this column
        /// </summary>
        public bool IsValueValid(object? value)
        {
            // Check nullable constraint
            if (value == null)
                return IsNullable;

            // Check data type compatibility
            return DataType switch
            {
                DataType.Integer => value is int or long,
                DataType.Long => value is int or long,
                DataType.Real => value is float or double or decimal,
                DataType.Text => value is string,
                _ => false
            };
        }

        /// <summary>
        /// Converts a value to the appropriate type for this column
        /// </summary>
        public object? ConvertValue(object? value)
        {
            if (value == null)
                return null;

            try
            {
                return DataType switch
                {
                    DataType.Integer => Convert.ToInt32(value),
                    DataType.Long => Convert.ToInt64(value),
                    DataType.Real => Convert.ToDouble(value),
                    DataType.Text => value.ToString(),
                    _ => value
                };
            }
            catch
            {
                throw new ArgumentException($"Cannot convert value '{value}' to {DataType}");
            }
        }

        public override string ToString()
        {
            var constraints = new List<string>();
            if (IsPrimaryKey) constraints.Add("PRIMARY KEY");
            if (IsAutoIncrement) constraints.Add("AUTO_INCREMENT");
            if (!IsNullable) constraints.Add("NOT NULL");
            
            var constraintStr = constraints.Count > 0 ? $" {string.Join(" ", constraints)}" : "";
            return $"{Name} {DataType.ToString().ToUpper()}{constraintStr}";
        }
    }
}
