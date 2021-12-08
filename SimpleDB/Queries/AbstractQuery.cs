using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal abstract class AbstractQuery
    {
        public Type EntityType { get; }

        protected AbstractQuery(Type entityType)
        {
            EntityType = entityType;
        }
    }
}
