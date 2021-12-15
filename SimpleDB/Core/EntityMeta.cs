using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class EntityMeta
    {
        public string EntityName { get; set; }

        public Type PrimaryKeyType { get; set; }

        public string PrimaryKeyName { get; set; }

        public IEnumerable<FieldMeta> FieldMetaCollection { get; set; }
    }
}
