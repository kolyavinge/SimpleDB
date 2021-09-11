using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleDB.Core
{
    internal class IndexHolder
    {
        private Dictionary<Type, List<AbstractIndex>> _indexes;

        public IndexHolder(IEnumerable<AbstractIndex> indexes)
        {
            _indexes = indexes.GroupBy(x => x.Meta.EntityType).ToDictionary(k => k.Key, v => v.ToList());
        }
    }
}
