using SimpleDB.Core;

namespace SimpleDB;

internal class FieldMappingValidator
{
    public void Validate<TEntity>(FieldMapping<TEntity> fieldMapping)
    {
        if (fieldMapping.Number == 0)
        {
            throw new DBEngineException("Number must be greater than zero");
        }
        if (fieldMapping.Settings.Compressed && fieldMapping.PropertyType.IsValueType)
        {
            throw new DBEngineException("Value type cannot be compressed");
        }
    }
}
