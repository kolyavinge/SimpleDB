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
}
