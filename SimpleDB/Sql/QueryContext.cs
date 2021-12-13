using System;
using System.Collections.Generic;
using System.Text;
using SimpleDB.Core;

namespace SimpleDB.Sql
{
    internal class QueryContext
    {
        public List<EntityMeta> EntityMetaCollection { get; set; }
    }

    internal class EntityMeta
    {
        public string EntityName { get; set; }

        public Type PrimaryKeyType { get; set; }

        public string PrimaryKeyName { get; set; }

        public IEnumerable<FieldMeta> FieldMetaCollection { get; set; }
    }
}
