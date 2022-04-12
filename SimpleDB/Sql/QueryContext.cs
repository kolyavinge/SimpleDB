using System.Collections.Generic;
using SimpleDB.Core;

namespace SimpleDB.Sql
{
    internal class QueryContext
    {
        public Dictionary<string, EntityMeta> EntityMetaDictionary { get; }

        public QueryContext(Dictionary<string, EntityMeta> entityMetaDictionary)
        {
            EntityMetaDictionary = entityMetaDictionary;
        }
    }
}
