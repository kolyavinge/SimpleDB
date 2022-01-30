using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Core
{
    internal class EntityMeta
    {
        public string EntityName { get; set; }

        public PrimaryKeyFieldMeta PrimaryKeyFieldMeta { get; set; }

        public IEnumerable<FieldMeta> FieldMetaCollection { get; set; }

        public static EntityMeta MakeFromMetaData(MetaData metaData)
        {
            return new EntityMeta
            {
                EntityName = metaData.EntityName,
                PrimaryKeyFieldMeta = new PrimaryKeyFieldMeta(metaData.PrimaryKeyName, metaData.PrimaryKeyType),
                FieldMetaCollection = metaData.FieldMetaCollection
            };
        }

        public IEnumerable<FieldMeta> GetPrimaryKeyAndFieldMetaCollection()
        {
            return new[] { PrimaryKeyFieldMeta }.Union(FieldMetaCollection);
        }
    }
}
