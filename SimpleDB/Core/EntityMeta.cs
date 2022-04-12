using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Core
{
    internal class EntityMeta
    {
        public string EntityName { get; private set; }

        public PrimaryKeyFieldMeta PrimaryKeyFieldMeta { get; private set; }

        public IEnumerable<FieldMeta> FieldMetaCollection { get; private set; }

        public static EntityMeta MakeFromMetaData(MetaData metaData)
        {
            return new EntityMeta(metaData.EntityName, new PrimaryKeyFieldMeta(metaData.PrimaryKeyName, metaData.PrimaryKeyType), metaData.FieldMetaCollection);
        }

        public EntityMeta(string entityName, PrimaryKeyFieldMeta primaryKeyFieldMeta, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            EntityName = entityName;
            PrimaryKeyFieldMeta = primaryKeyFieldMeta;
            FieldMetaCollection = fieldMetaCollection;
        }

        public IEnumerable<FieldMeta> GetPrimaryKeyAndFieldMetaCollection()
        {
            return new[] { PrimaryKeyFieldMeta }.Union(FieldMetaCollection);
        }
    }
}
