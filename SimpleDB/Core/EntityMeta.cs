using System;
using System.Collections.Generic;

namespace SimpleDB.Core
{
    internal class EntityMeta
    {
        public string EntityName { get; set; }

        public Type PrimaryKeyType { get; set; }

        public string PrimaryKeyName { get; set; }

        public IEnumerable<FieldMeta> FieldMetaCollection { get; set; }

        public static EntityMeta MakeFromMetaData(MetaData metaData)
        {
            return new EntityMeta
            {
                EntityName = metaData.EntityName,
                PrimaryKeyType = metaData.PrimaryKeyType,
                PrimaryKeyName = metaData.PrimaryKeyName,
                FieldMetaCollection = metaData.FieldMetaCollection
            };
        }
    }
}
