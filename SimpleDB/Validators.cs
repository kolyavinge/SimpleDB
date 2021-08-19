using System;
using System.Collections.Generic;
using System.Text;
using SimpleDB.Core;

namespace SimpleDB
{
    internal class FieldMappingValidator
    {
        public void Validate<TEntity>(FieldMapping<TEntity> fieldMapping)
        {
            if (fieldMapping.Settings.Compressed && fieldMapping.PropertyType.IsValueType)
            {
                throw new DBEngineException("Value type cannot be compressed");
            }
        }
    }
}
